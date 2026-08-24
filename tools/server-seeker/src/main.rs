mod cidr;
mod config;
mod db;
mod domains;
mod masscan;
mod pg;
mod ping;
mod rescanner;

use anyhow::{Context, Result, bail};
use cidr::{display_ip, expand_cidr, is_private, parse_cidr};
use clap::Parser;
use config::{
    Mode, RescanPriority, default_masscan_config_path, default_watch_interval,
    load_env_config, merge_cli_env, scan_source_label, validate_config,
};
use db::SqliteStore;
use domains::{DomainScanConfig, run_domains};
use masscan::{MasscanOptions, run_masscan};
use pg::PostgresStore;
use ping::PingResult;
use rescanner::{RescanConfig, run_rescan};
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::{Instant, sleep};

#[derive(Parser, Debug)]
#[command(
    name = "server-seeker",
    about = "ServerSeekerV2-style discovery: masscan + domain scan ??? Postgres ??? Discord bridge"
)]
struct Args {
    /// scanner | rescanner | cidr | watch (auto IP + domain loop)
    #[arg(long, value_enum, default_value_t = Mode::Watch)]
    mode: Mode,

    #[arg(long = "query")]
    queries: Vec<String>,

    /// Domain hostnames to resolve and ping (also SEEKER_SEEDS / SEEKER_DOMAINS)
    #[arg(long = "seed")]
    seeds: Vec<String>,

    #[arg(long = "cidr")]
    cidrs: Vec<String>,

    #[arg(long, env = "SEEKER_MASSCAN_CONFIG")]
    masscan_config: Option<PathBuf>,

    #[arg(long, default_value_t = 25565)]
    port: u16,

    #[arg(long, default_value = "64")]
    concurrency: usize,

    #[arg(long, default_value = "4000")]
    timeout_ms: u64,

    #[arg(long, default_value = "200")]
    rate: u32,

    #[arg(long)]
    limit: Option<usize>,

    #[arg(long, default_value = "server-seeker.db")]
    db: PathBuf,

    #[arg(long, env = "SEEKER_DATABASE_URL")]
    postgres: Option<String>,

    #[arg(long)]
    verbose: bool,

    #[arg(long)]
    i_understand: bool,

    /// Seconds between watch cycles (also SEEKER_WATCH_INTERVAL / _SECS)
    #[arg(long)]
    watch: Option<u64>,

    #[arg(long)]
    rescan_limit: Option<usize>,
}

#[derive(Clone)]
pub(crate) struct ScanConfig {
    port: u16,
    timeout: Duration,
    queries: Vec<String>,
    verbose: bool,
    scan_source: String,
}

pub(crate) enum Store {
    Sqlite(Arc<Mutex<SqliteStore>>),
    Postgres(Arc<PostgresStore>),
}

impl Store {
    fn clone_store(&self) -> Self {
        match self {
            Store::Sqlite(s) => Store::Sqlite(s.clone()),
            Store::Postgres(p) => Store::Postgres(p.clone()),
        }
    }

    async fn list_servers(
        &self,
        priority: RescanPriority,
        limit: Option<usize>,
    ) -> Result<Vec<(IpAddr, u16)>> {
        let rows = match self {
            Store::Sqlite(sqlite) => {
                let db = sqlite.lock().await;
                db.list_servers(priority, limit)?
            }
            Store::Postgres(pg) => pg.list_servers(priority, limit).await?,
        };
        Ok(rows
            .into_iter()
            .filter_map(|(ip, port)| ip.parse::<IpAddr>().ok().map(|addr| (addr, port)))
            .collect())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let env_cfg = load_env_config()?;

    let mode = if std::env::args().any(|a| a == "--mode") {
        args.mode
    } else {
        env_cfg.mode
    };

    let (queries, seeds, cidrs) =
        merge_cli_env(&args.queries, &args.seeds, &args.cidrs, &env_cfg);
    let watch_interval = args
        .watch
        .map(Duration::from_secs)
        .or(env_cfg.watch_interval)
        .unwrap_or(default_watch_interval());

    let masscan_config = args
        .masscan_config
        .as_ref()
        .map(|p| p.to_string_lossy().into_owned())
        .or(env_cfg.masscan_config.clone())
        .or_else(|| {
            let default = default_masscan_config_path();
            if default.is_file() {
                Some(default.to_string_lossy().into_owned())
            } else {
                None
            }
        });

    validate_config(mode, &seeds, &cidrs, masscan_config.as_deref())?;

    let postgres_url = args.postgres.clone().or(env_cfg.database_url.clone());
    let store = if let Some(url) = postgres_url {
        Store::Postgres(Arc::new(PostgresStore::connect(&url).await?))
    } else {
        let path = env_cfg
            .sqlite_path
            .map(PathBuf::from)
            .unwrap_or(args.db.clone());
        Store::Sqlite(Arc::new(Mutex::new(SqliteStore::open(&path)?)))
    };

    let runtime = RuntimeConfig {
        mode,
        queries,
        seeds,
        cidrs,
        port: pick(args.port, env_cfg.port, 25565),
        concurrency: pick(args.concurrency, env_cfg.concurrency, 64),
        timeout: if args.timeout_ms == 4000 {
            env_cfg.timeout
        } else {
            Duration::from_millis(args.timeout_ms)
        },
        rate: pick(args.rate, env_cfg.rate, 200),
        limit: args.limit.or(env_cfg.limit),
        verbose: args.verbose,
        i_understand: args.i_understand || env_cfg.i_understand,
        masscan_config,
        masscan_bin: env_cfg.masscan_bin,
        masscan_sudo: env_cfg.masscan_sudo,
        rescan_priority: env_cfg.rescan_priority,
        rescan_limit: args.rescan_limit.or(env_cfg.rescan_limit),
        watch_interval,
    };

    if needs_i_understand(&runtime) && !runtime.i_understand {
        bail!(
            "IP scanning requires --i-understand or SEEKER_I_UNDERSTAND=true.\n\
             Only scan ranges you own or have explicit permission to probe."
        );
    }

    match runtime.mode {
        Mode::Watch => run_watch(&runtime, &store).await,
        Mode::Scanner | Mode::Rescanner | Mode::Cidr => run_once(&runtime, &store).await,
    }
}

struct RuntimeConfig {
    mode: Mode,
    queries: Vec<String>,
    seeds: Vec<String>,
    cidrs: Vec<String>,
    port: u16,
    concurrency: usize,
    timeout: Duration,
    rate: u32,
    limit: Option<usize>,
    verbose: bool,
    i_understand: bool,
    masscan_config: Option<String>,
    masscan_bin: String,
    masscan_sudo: bool,
    rescan_priority: RescanPriority,
    rescan_limit: Option<usize>,
    watch_interval: Duration,
}

fn needs_i_understand(runtime: &RuntimeConfig) -> bool {
    runtime.masscan_config.is_some()
        || !runtime.cidrs.is_empty()
        || matches!(runtime.mode, Mode::Scanner | Mode::Cidr)
}

fn pick<T: PartialEq + Copy>(cli: T, env: T, default: T) -> T {
    if cli != default {
        cli
    } else {
        env
    }
}

async fn run_watch(runtime: &RuntimeConfig, store: &Store) -> Result<()> {
    eprintln!(
        "watch: auto-scan every {}s ??? masscan={}, domains={}, cidrs={}, rescan",
        runtime.watch_interval.as_secs(),
        runtime.masscan_config.is_some(),
        runtime.seeds.len(),
        runtime.cidrs.len()
    );

    loop {
        if let Err(error) = run_watch_cycle(runtime, store).await {
            eprintln!("watch: cycle error (continuing): {error:#}");
        }
        eprintln!(
            "watch: sleeping {}s until next cycle",
            runtime.watch_interval.as_secs()
        );
        sleep(runtime.watch_interval).await;
    }
}

async fn run_watch_cycle(runtime: &RuntimeConfig, store: &Store) -> Result<()> {
    eprintln!("watch: --- cycle start ---");

    if runtime.masscan_config.is_some() {
        if let Err(error) = run_scanner(runtime, store).await {
            eprintln!("watch: masscan pass failed: {error:#}");
        }
    }

    if !runtime.seeds.is_empty() {
        if let Err(error) = run_domains_pass(runtime, store).await {
            eprintln!("watch: domain pass failed: {error:#}");
        }
    }

    if !runtime.cidrs.is_empty() {
        if let Err(error) = run_cidr(runtime, store).await {
            eprintln!("watch: cidr pass failed: {error:#}");
        }
    }

    if let Err(error) = run_rescanner_mode(runtime, store).await {
        eprintln!("watch: rescan pass failed: {error:#}");
    }

    eprintln!("watch: --- cycle done ---");
    Ok(())
}

async fn run_once(runtime: &RuntimeConfig, store: &Store) -> Result<()> {
    match runtime.mode {
        Mode::Scanner => run_scanner(runtime, store).await,
        Mode::Rescanner => run_rescanner_mode(runtime, store).await,
        Mode::Cidr => run_cidr(runtime, store).await,
        Mode::Watch => run_watch_cycle(runtime, store).await,
    }
}

async fn run_domains_pass(runtime: &RuntimeConfig, store: &Store) -> Result<()> {
    // Seed domains are explicitly configured ??? store every server that responds,
    // regardless of MOTD query (masscan pass still uses SEEKER_QUERIES).
    let scan = ScanConfig {
        port: runtime.port,
        timeout: runtime.timeout,
        queries: Vec::new(),
        verbose: runtime.verbose,
        scan_source: scan_source_label(Mode::Watch, Some("domain")),
    };
    let domains = DomainScanConfig {
        domains: runtime.seeds.clone(),
        port: runtime.port,
        timeout: runtime.timeout,
        concurrency: runtime.concurrency,
        rate: runtime.rate,
    };
    run_domains(store, &domains, &scan).await?;
    Ok(())
}

async fn run_scanner(runtime: &RuntimeConfig, store: &Store) -> Result<()> {
    let config_path = runtime
        .masscan_config
        .as_deref()
        .context("masscan config required for scanner mode")?;

    eprintln!(
        "scanner: masscan ({config_path}) ??? status ping (concurrency {}, rate {}/s, queries={:?})",
        runtime.concurrency,
        if runtime.rate == 0 {
            "unlimited".to_string()
        } else {
            runtime.rate.to_string()
        },
        runtime.queries
    );

    let scan = ScanConfig {
        port: runtime.port,
        timeout: runtime.timeout,
        queries: runtime.queries.clone(),
        verbose: runtime.verbose,
        scan_source: scan_source_label(Mode::Scanner, None),
    };

    let semaphore = Arc::new(Semaphore::new(runtime.concurrency));
    let rate_state = Arc::new(Mutex::new(RateLimiter::new(runtime.rate)));
    let store = store.clone_store();
    let stats = Arc::new(Mutex::new(ScanStats::default()));

    let masscan_opts = MasscanOptions {
        bin: runtime.masscan_bin.clone(),
        config_path: config_path.to_string(),
        use_sudo: runtime.masscan_sudo,
    };

    let stats_for_cb = stats.clone();
    run_masscan(&masscan_opts, move |address, port| {
        let semaphore = semaphore.clone();
        let rate_state = rate_state.clone();
        let store = store.clone_store();
        let scan = scan.clone();
        let stats = stats_for_cb.clone();
        async move {
            {
                let mut s = stats.lock().await;
                s.probed += 1;
            }
            let _permit = semaphore.acquire_owned().await?;
            rate_state.lock().await.wait().await;
            let ip = IpAddr::V4(address);
            let ip_str = display_ip(ip);
            let ping = ping::status_ping(&ip_str, port, scan.timeout).await?;
            let (is_match, inserted) = process_ping(&store, ip, port, ping, &scan, None).await?;
            if is_match {
                let mut s = stats.lock().await;
                s.matched += 1;
                s.new_count += inserted;
            }
            Ok(())
        }
    })
    .await?;

    let final_stats = stats.lock().await;
    eprintln!(
        "scanner: done, {} masscan hits, {} matched, {} new",
        final_stats.probed, final_stats.matched, final_stats.new_count
    );
    Ok(())
}

#[derive(Default)]
struct ScanStats {
    probed: usize,
    matched: usize,
    new_count: usize,
}

async fn run_cidr(runtime: &RuntimeConfig, store: &Store) -> Result<()> {
    let targets = collect_cidr_targets(runtime)?;
    if targets.is_empty() {
        bail!("no scan targets after CIDR expansion");
    }

    let has_public = targets.iter().any(|ip| !is_private(*ip));
    if has_public && !runtime.i_understand {
        bail!("cidr scan includes public IPs; set --i-understand");
    }

    eprintln!(
        "cidr: probing {} host(s) on port {} (concurrency {}, rate {}/s)",
        targets.len(),
        runtime.port,
        runtime.concurrency,
        runtime.rate
    );

    let scan = ScanConfig {
        port: runtime.port,
        timeout: runtime.timeout,
        queries: runtime.queries.clone(),
        verbose: runtime.verbose,
        scan_source: scan_source_label(
            Mode::Cidr,
            runtime.cidrs.first().map(String::as_str),
        ),
    };

    let semaphore = Arc::new(Semaphore::new(runtime.concurrency));
    let rate_state = Arc::new(Mutex::new(RateLimiter::new(runtime.rate)));
    let mut handles = Vec::new();

    for ip in targets {
        let permit = semaphore.clone().acquire_owned().await?;
        let store = store.clone_store();
        let scan = scan.clone();
        let rate_state = rate_state.clone();
        handles.push(tokio::spawn(async move {
            let _permit = permit;
            rate_state.lock().await.wait().await;
            let ip_str = display_ip(ip);
            let ping = ping::status_ping(&ip_str, scan.port, scan.timeout).await?;
            process_ping(&store, ip, scan.port, ping, &scan, None).await
        }));
    }

    let mut matched = 0usize;
    let mut new_count = 0usize;
    for handle in handles {
        if let Ok(Ok((is_match, inserted))) = handle.await {
            if is_match {
                matched += 1;
            }
            new_count += inserted;
        }
    }

    eprintln!("cidr: done, {matched} matched, {new_count} new");
    Ok(())
}

async fn run_rescanner_mode(runtime: &RuntimeConfig, store: &Store) -> Result<()> {
    let scan = ScanConfig {
        port: runtime.port,
        timeout: runtime.timeout,
        queries: runtime.queries.clone(),
        verbose: runtime.verbose,
        scan_source: scan_source_label(Mode::Rescanner, None),
    };
    let rescan = RescanConfig {
        timeout: runtime.timeout,
        concurrency: runtime.concurrency,
        rate: runtime.rate,
        priority: runtime.rescan_priority,
        limit: runtime.rescan_limit,
    };
    run_rescan(store, &rescan, &scan).await?;
    Ok(())
}

fn collect_cidr_targets(runtime: &RuntimeConfig) -> Result<Vec<IpAddr>> {
    let mut nets = Vec::new();
    for cidr in &runtime.cidrs {
        nets.push(parse_cidr(cidr)?);
    }
    let per_net_limit = runtime.limit.map(|total| {
        let count = nets.len().max(1);
        (total + count - 1) / count
    });
    let mut targets = Vec::new();
    for net in nets {
        targets.append(&mut expand_cidr(&net, per_net_limit)?);
    }
    if let Some(limit) = runtime.limit {
        targets.truncate(limit);
    }
    targets.sort();
    targets.dedup();
    Ok(targets)
}

/// Returns (matched, new_rows_inserted)
pub(crate) async fn process_ping(
    store: &Store,
    ip: IpAddr,
    port: u16,
    ping: PingResult,
    scan: &ScanConfig,
    hostname: Option<&str>,
) -> Result<(bool, usize)> {
    let haystack = format!(
        "{} {}",
        ping.motd.to_lowercase(),
        ping.version_name.to_lowercase()
    );
    let mut matched_queries: Vec<String> = if scan.queries.is_empty() {
        vec!["*".to_string()]
    } else {
        scan
            .queries
            .iter()
            .filter(|q| haystack.contains(&q.to_lowercase()))
            .cloned()
            .collect()
    };

    // Domain seeds: store any server that responds on 25565, even if MOTD lacks SEEKER_QUERIES.
    if matched_queries.is_empty() {
        if let Some(host) = hostname {
            matched_queries.push(format!("seed:{host}"));
        }
    }

    let is_match = !matched_queries.is_empty();
    if scan.verbose || is_match {
        print_hit(&display_ip(ip), port, hostname, &ping, &matched_queries);
    }

    if !is_match {
        return Ok((false, 0));
    }

    let mut inserted = 0usize;
    for query in &matched_queries {
        if save_finding(store, ip, port, hostname, &ping, query, &scan.scan_source).await? {
            inserted += 1;
        }
    }
    Ok((true, inserted))
}

async fn save_finding(
    store: &Store,
    ip: IpAddr,
    port: u16,
    hostname: Option<&str>,
    ping: &PingResult,
    query: &str,
    scan_source: &str,
) -> Result<bool> {
    let ip_str = display_ip(ip);
    let matched = if query == "*" { None } else { Some(query) };
    match store {
        Store::Sqlite(sqlite) => {
            let db = sqlite.lock().await;
            db.insert_finding(
                &ip_str,
                port,
                &ping.motd,
                &ping.version_name,
                ping.protocol,
                ping.players_online,
                ping.players_max,
                hostname,
                matched,
                scan_source,
                &ping.raw_json,
            )
        }
        Store::Postgres(pg) => {
            let result = pg
                .upsert_finding(
                    &ip_str,
                    port as i32,
                    hostname,
                    &ping.motd,
                    &ping.version_name,
                    ping.protocol,
                    ping.players_online as i32,
                    ping.players_max as i32,
                    matched,
                    scan_source,
                    &ping.raw_json,
                )
                .await?;
            if result.is_new {
                let host = hostname.unwrap_or("-");
                eprintln!("[NEW] {ip_str}:{port} ({host}) matched {query}");
            }
            Ok(result.is_new)
        }
    }
}

fn print_hit(
    ip: &str,
    port: u16,
    hostname: Option<&str>,
    ping: &PingResult,
    queries: &[String],
) {
    let tag = if queries.is_empty() { "open" } else { "MATCH" };
    let q = if queries.is_empty() {
        "-".to_string()
    } else {
        queries.join(", ")
    };
    let host = hostname.unwrap_or("-");
    println!(
        "[{tag}] {ip}:{port} ({host}) | {} | proto {} | {}/{} players | query: {q} | motd: {}",
        ping.version_name,
        ping.protocol,
        ping.players_online,
        ping.players_max,
        ping.motd
    );
}

pub(crate) struct RateLimiter {
    per_second: u32,
    window_start: Instant,
    count: u32,
}

impl RateLimiter {
    pub fn new(per_second: u32) -> Self {
        Self {
            per_second,
            window_start: Instant::now(),
            count: 0,
        }
    }

    pub async fn wait(&mut self) {
        if self.per_second == 0 {
            return;
        }
        let elapsed = self.window_start.elapsed();
        if elapsed >= Duration::from_secs(1) {
            self.window_start = Instant::now();
            self.count = 0;
        }
        if self.count >= self.per_second {
            let remaining = Duration::from_secs(1) - elapsed;
            sleep(remaining).await;
            self.window_start = Instant::now();
            self.count = 0;
        }
        self.count += 1;
    }
}

