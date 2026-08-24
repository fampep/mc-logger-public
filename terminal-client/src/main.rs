//! Live event gateway: one TCP listener, multiplexed by server key.

mod hub;

use std::collections::HashSet;
use std::env;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use tracing_subscriber::EnvFilter;

use crate::hub::{Gateway, GatewayConfig, GATEWAY_VERSION};

const HELP: &str = r#"terminal-client — live Minecraft event gateway

One TCP listener multiplexes every server key: loggers push events, the Discord
bot and mc-tail read them back.

USAGE
  terminal-client                 run the gateway (configured by environment)
  terminal-client --help          this text
  terminal-client --version       print the build

ENVIRONMENT (all optional; a bare port is accepted for LISTEN)
  LISTEN=127.0.0.1:9700      address to listen on
  BUFFER=500                 events kept per server key for replay/resume
  SERVER_KEYS=a,b,c          only accept these keys (unset = accept any)
  AUTH_TOKEN=secret          require this token from every client
                             (clients read it from EVENT_STREAM_TOKEN)
  STATS_INTERVAL_SECS=300    how often to log a one-line health summary (0 = off)
  PING_SECS=30               keepalive interval for idle consumers
  PRODUCER_IDLE_SECS=180     drop a producer silent for this long (0 = never)
  HELLO_TIMEOUT_SECS=10      how long a new connection may take to identify itself
  WRITE_TIMEOUT_SECS=15      drop a consumer that stops reading for this long
  SAY_PER_MIN=20             lines one client may relay into the game per minute
  MAX_CONNS_PER_KEY=32       connection cap per server key
  MAX_KEYS=64                distinct server keys held at once
  EVICT_AFTER_SECS=3600      forget an unused key's buffer after this long
  RUST_LOG=terminal_client=debug   verbose logging

INSPECT A RUNNING GATEWAY
  mc-tail --status           health of every server key
  mc-tail <server-key>       follow one feed
"#;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let _ = dotenvy::dotenv();

    for arg in env::args().skip(1) {
        match arg.as_str() {
            "-h" | "--help" => {
                print!("{HELP}");
                return Ok(());
            }
            "-V" | "--version" => {
                println!("{GATEWAY_VERSION}");
                return Ok(());
            }
            other => {
                eprintln!("terminal-client: unexpected argument \"{other}\"\n");
                print!("{HELP}");
                std::process::exit(2);
            }
        }
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "terminal_client=info,warn".into()),
        )
        .init();

    let listen = parse_listen(&optional("LISTEN", "127.0.0.1:9700"))?;
    let cfg = load_config()?;
    let stats_interval = secs("STATS_INTERVAL_SECS", 300);

    let gateway = Arc::new(Gateway::new(cfg));
    let c = gateway.config();
    tracing::info!("[gateway] {GATEWAY_VERSION} listening on {listen}");
    tracing::info!(
        "[gateway] buffer={} auth={} keys={} max_conns_per_key={}",
        c.buffer_cap,
        if c.token.is_some() {
            "required"
        } else {
            "open (set AUTH_TOKEN to require one)"
        },
        match &c.allowed_keys {
            Some(keys) => {
                let mut list: Vec<&str> = keys.iter().map(String::as_str).collect();
                list.sort_unstable();
                list.join(",")
            }
            None => "any (set SERVER_KEYS to restrict)".to_string(),
        },
        c.max_conns_per_key
    );

    // Housekeeping: refresh event rates, evict unused keys, and log a summary
    // that answers "is the feed actually flowing?" without attaching mc-tail.
    let housekeeper = Arc::clone(&gateway);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(30));
        let mut since_summary = Duration::ZERO;
        loop {
            tick.tick().await;
            housekeeper.maintain();
            since_summary += Duration::from_secs(30);
            if stats_interval > 0 && since_summary.as_secs() >= stats_interval {
                since_summary = Duration::ZERO;
                log_summary(&housekeeper);
            }
        }
    });

    let serving = Arc::clone(&gateway);
    tokio::select! {
        result = serving.serve(listen) => result,
        _ = shutdown_signal() => {
            log_summary(&gateway);
            tracing::info!("[gateway] shutting down");
            Ok(())
        }
    }
}

fn log_summary(gateway: &Gateway) {
    let status = gateway.status();
    if status.servers.is_empty() {
        tracing::info!(
            "[gateway] up {}s, no server keys connected yet — check SERVER_KEY= on the loggers",
            status.uptime_secs
        );
        return;
    }
    for server in &status.servers {
        tracing::info!(
            "[gateway] {} — {} | producers={} consumers={} {:.1} ev/min in={} out={} dropped={} evicted={} buffered={}",
            server.server,
            server.health(),
            server.producers,
            server.consumers,
            server.events_per_min,
            server.events_in,
            server.events_out,
            server.dropped,
            server.ring_evicted,
            server.buffered,
        );
    }
}

fn load_config() -> eyre::Result<GatewayConfig> {
    let defaults = GatewayConfig::default();
    let token = optional("AUTH_TOKEN", "");
    let token = if token.is_empty() {
        mc_stream::token_from_env()
    } else {
        Some(token)
    };
    let keys: HashSet<String> = optional("SERVER_KEYS", "")
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let producer_idle = secs("PRODUCER_IDLE_SECS", defaults.producer_idle.as_secs());

    Ok(GatewayConfig {
        buffer_cap: count("BUFFER", defaults.buffer_cap as u64, 10, 100_000)? as usize,
        token,
        allowed_keys: if keys.is_empty() { None } else { Some(keys) },
        hello_timeout: Duration::from_secs(
            secs("HELLO_TIMEOUT_SECS", defaults.hello_timeout.as_secs()).max(1),
        ),
        // 0 disables the idle drop; anything shorter than a keepalive round
        // would drop healthy producers.
        producer_idle: if producer_idle == 0 {
            Duration::from_secs(u64::MAX / 2)
        } else {
            Duration::from_secs(producer_idle.max(mc_stream::KEEPALIVE.as_secs() * 2))
        },
        ping_interval: Duration::from_secs(
            secs("PING_SECS", defaults.ping_interval.as_secs()).max(5),
        ),
        write_timeout: Duration::from_secs(
            secs("WRITE_TIMEOUT_SECS", defaults.write_timeout.as_secs()).max(1),
        ),
        max_conns_per_key: count(
            "MAX_CONNS_PER_KEY",
            defaults.max_conns_per_key as u64,
            2,
            10_000,
        )? as usize,
        max_keys: count("MAX_KEYS", defaults.max_keys as u64, 1, 10_000)? as usize,
        evict_after: Duration::from_secs(
            secs("EVICT_AFTER_SECS", defaults.evict_after.as_secs()).max(60),
        ),
        say_per_min: count("SAY_PER_MIN", defaults.say_per_min as u64, 1, 10_000)? as u32,
    })
}

/// Accepts `host:port`, a bare port, or `:port`, so a typo in the unit file
/// fails with an explanation instead of a raw parse error.
fn parse_listen(raw: &str) -> eyre::Result<SocketAddr> {
    let raw = raw.trim();
    if let Ok(addr) = raw.parse::<SocketAddr>() {
        return Ok(addr);
    }
    if let Ok(port) = raw.trim_start_matches(':').parse::<u16>() {
        return Ok(SocketAddr::from(([127, 0, 0, 1], port)));
    }
    raw.to_socket_addrs()
        .ok()
        .and_then(|mut addrs| addrs.next())
        .ok_or_else(|| {
            eyre::eyre!(
                "LISTEN=\"{raw}\" is not an address. Use HOST:PORT (127.0.0.1:9700), \
                 or just a port (9700)."
            )
        })
}

fn optional(name: &str, fallback: &str) -> String {
    match env::var(name) {
        Ok(v) if !v.trim().is_empty() => v.trim().to_string(),
        _ => fallback.to_string(),
    }
}

fn secs(name: &str, fallback: u64) -> u64 {
    match env::var(name) {
        Ok(raw) if !raw.trim().is_empty() => raw.trim().parse().unwrap_or_else(|_| {
            tracing::warn!("[gateway] {name}=\"{raw}\" is not a number; using {fallback}");
            fallback
        }),
        _ => fallback,
    }
}

fn count(name: &str, fallback: u64, min: u64, max: u64) -> eyre::Result<u64> {
    let raw = optional(name, "");
    if raw.is_empty() {
        return Ok(fallback);
    }
    let value: u64 = raw
        .parse()
        .map_err(|_| eyre::eyre!("{name}=\"{raw}\" must be a number between {min} and {max}"))?;
    Ok(value.clamp(min, max))
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(sig) => sig,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::parse_listen;

    #[test]
    fn listen_accepts_the_shapes_people_actually_type() {
        assert_eq!(parse_listen("127.0.0.1:9700").unwrap().port(), 9700);
        assert_eq!(parse_listen("9700").unwrap().port(), 9700);
        assert_eq!(parse_listen(":9700").unwrap().port(), 9700);
        assert!(parse_listen("not an address").is_err());
    }
}
