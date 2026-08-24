//! Attach to the live event gateway and print events (debug / ops).

use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::time::Duration;

use chrono::{DateTime, Local, Utc};
use mc_stream::{fetch_status, GatewayStatus, StreamEvent, StreamSubscriber, SubscribeOptions};

const DEFAULT_ADDR: &str = "127.0.0.1:9700";
const DEFAULT_REPLAY: u32 = 25;
/// Say something if the feed goes quiet for this long, rather than looking hung.
const QUIET_NOTICE: Duration = Duration::from_secs(90);

const HELP: &str = r#"mc-tail — follow a live Minecraft feed from the gateway

USAGE
  mc-tail [SERVER_KEY] [OPTIONS]

  With no SERVER_KEY, mc-tail asks the gateway which keys it has: if there is
  exactly one it follows that, otherwise it lists them.

OPTIONS
  -a, --addr HOST:PORT   gateway address        [env STREAM_ADDR, default 127.0.0.1:9700]
  -n, --replay N         backfill N buffered events on connect  [default 25, 0 = live only]
      --since SEQ        backfill everything after sequence SEQ instead
  -k, --kind LIST        only these kinds: chat,join,leave,death,advancement,server
  -p, --player NAME      only lines naming NAME (comma-separated, repeatable)
  -g, --grep TEXT        only lines containing TEXT (case-insensitive)
  -t, --time STYLE       clock | rel | full | none                [default clock]
      --token TOKEN      gateway token          [env EVENT_STREAM_TOKEN]
      --json             print raw JSON lines instead of formatted text
      --seq              show sequence numbers
      --badges           kind badges instead of the server's own colours
      --no-color         disable colour (also honours NO_COLOR)
  -q, --quiet            suppress connection notices
      --say TEXT         say one line in game through the logger, then exit
      --as NAME          who a --say line is from                  [default console]
  -s, --status           print gateway health and exit
  -l, --list             print server keys and exit
  -h, --help             this text
  -V, --version          print the build

EXAMPLES
  mc-tail                             follow the only key on a local gateway
  mc-tail ninebninet                  follow one server
  mc-tail ninebninet -k death,join    just deaths and joins
  mc-tail 2b2t -p Herobrine -n 200    everything naming a player, with backfill
  mc-tail --status                    is the feed flowing? who is connected?
  mc-tail 2b2t --say "brb"            speak in game from the console
  mc-tail 2b2t --json | jq .          pipe the raw stream somewhere else

  Legacy form still works: mc-tail 127.0.0.1:9700 ninebninet 50
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeStyle {
    Clock,
    Rel,
    Full,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Mode {
    Follow,
    Status,
    List,
    /// Send one line into the game and exit.
    Say(String),
}

struct Args {
    addr: String,
    server: Option<String>,
    replay: u32,
    since: Option<u64>,
    kinds: Vec<String>,
    players: Vec<String>,
    grep: Option<String>,
    token: Option<String>,
    time: TimeStyle,
    json: bool,
    show_seq: bool,
    color: bool,
    quiet: bool,
    /// Show mc-tail's own kind badges instead of the server's rendering.
    badges: bool,
    /// Name a `--say` line is attributed to.
    say_as: Option<String>,
    mode: Mode,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            addr: std::env::var("STREAM_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.into()),
            server: std::env::var("SERVER_KEY").ok().filter(|s| !s.is_empty()),
            replay: std::env::var("REPLAY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_REPLAY),
            since: None,
            kinds: Vec::new(),
            players: Vec::new(),
            grep: None,
            token: mc_stream::token_from_env(),
            time: TimeStyle::Clock,
            json: false,
            show_seq: false,
            color: std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none(),
            quiet: false,
            badges: false,
            say_as: None,
            mode: Mode::Follow,
        }
    }
}

#[tokio::main]
async fn main() {
    // Reconnects and gateway rejections are logged by mc-stream; without a
    // subscriber they vanish and a refused mc-tail just looks hung.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mc_stream=warn,warn".into()),
        )
        .with_target(false)
        .without_time()
        .with_writer(std::io::stderr)
        .init();

    let args = match parse_args(std::env::args().skip(1).collect()) {
        Ok(Some(args)) => args,
        Ok(None) => return,
        Err(err) => {
            eprintln!("mc-tail: {err}\n\nTry `mc-tail --help`.");
            std::process::exit(2);
        }
    };
    if let Err(err) = run(args).await {
        eprintln!("mc-tail: {err}");
        std::process::exit(1);
    }
}

async fn run(mut args: Args) -> eyre::Result<()> {
    match args.mode {
        Mode::Status => {
            let status = status_or_hint(&args).await?;
            print!("{}", render_status(&status, args.color));
            return Ok(());
        }
        Mode::List => {
            let status = status_or_hint(&args).await?;
            for server in &status.servers {
                println!("{}", server.server);
            }
            return Ok(());
        }
        // Cloned first: matching by value would partially move `args`, which
        // say_once still needs for the address and token.
        Mode::Say(ref text) => {
            let text = text.clone();
            return say_once(&args, &text).await;
        }
        Mode::Follow => {}
    }

    // No key given: ask the gateway rather than guessing "default" and showing
    // an empty feed forever.
    if args.server.is_none() {
        let status = status_or_hint(&args).await?;
        match status.servers.len() {
            0 => eyre::bail!(
                "the gateway at {} has no server keys yet — no logger has connected. \
                 Check SERVER_KEY= and EVENT_STREAM_ADDR= in azalea-bot/.env",
                args.addr
            ),
            1 => {
                let only = status.servers[0].server.clone();
                if !args.quiet {
                    eprintln!("{}", dim(&format!("no key given; following the only one: {only}"), args.color));
                }
                args.server = Some(only);
            }
            _ => {
                let keys: Vec<&str> = status.servers.iter().map(|s| s.server.as_str()).collect();
                eyre::bail!(
                    "the gateway at {} carries {} server keys: {}.\nPick one, e.g. `mc-tail {}`.",
                    args.addr,
                    keys.len(),
                    keys.join(", "),
                    keys[0]
                );
            }
        }
    }
    let server = args.server.clone().expect("server key resolved above");

    // Fail fast with a useful message instead of retry-looping in the
    // background against a gateway that will never accept us.
    if let Ok(status) = fetch_status(&args.addr, args.token.clone()).await {
        if !status.allowed_keys.is_empty() && !status.allowed_keys.contains(&server) {
            eyre::bail!(
                "the gateway at {} does not accept \"{server}\". Its SERVER_KEYS are: {}",
                args.addr,
                status.allowed_keys.join(", ")
            );
        }
        if let Some(row) = status.servers.iter().find(|s| s.server == server) {
            if !args.quiet && row.producers == 0 {
                eprintln!(
                    "{}",
                    warn(
                        &format!("no logger is producing for \"{server}\" — you will only see backfill"),
                        args.color
                    )
                );
            }
        } else if !status.servers.is_empty() {
            let keys: Vec<&str> = status.servers.iter().map(|s| s.server.as_str()).collect();
            eyre::bail!(
                "\"{server}\" is not on the gateway at {}. Known keys: {}",
                args.addr,
                keys.join(", ")
            );
        }
    }

    if !args.quiet {
        let mut how = vec![match args.since {
            Some(seq) => format!("since seq {seq}"),
            None if args.replay > 0 => format!("replay {}", args.replay),
            None => "live only".to_string(),
        }];
        if !args.kinds.is_empty() {
            how.push(format!("kinds {}", args.kinds.join(",")));
        }
        if !args.players.is_empty() {
            how.push(format!("players {}", args.players.join(",")));
        }
        if let Some(grep) = &args.grep {
            how.push(format!("grep \"{grep}\""));
        }
        eprintln!(
            "{}",
            dim(
                &format!("mc-tail {server} @ {} ({}) — Ctrl-C to stop", args.addr, how.join(", ")),
                args.color
            )
        );
    }

    let mut sub = StreamSubscriber::spawn_with(
        args.addr.clone(),
        server.clone(),
        SubscribeOptions {
            replay: args.replay,
            since: args.since,
            token: args.token.clone(),
            client: Some("mc-tail".into()),
        },
    );

    let mut seen: u64 = 0;
    let mut shown: u64 = 0;
    let mut by_kind: BTreeMap<String, u64> = BTreeMap::new();

    loop {
        tokio::select! {
            event = sub.rx.recv() => {
                let Some(event) = event else { break };
                seen += 1;
                if !passes(&event, &args) {
                    continue;
                }
                shown += 1;
                *by_kind.entry(event.kind().to_string()).or_default() += 1;
                if args.json {
                    match serde_json::to_string(&event) {
                        Ok(line) => println!("{line}"),
                        Err(err) => eprintln!("mc-tail: cannot re-encode event: {err}"),
                    }
                } else {
                    println!("{}", format_event(&event, &args));
                }
            }
            _ = tokio::time::sleep(QUIET_NOTICE) => {
                if !args.quiet {
                    let state = if sub.connected() { "connected" } else { "not connected" };
                    eprintln!(
                        "{}",
                        dim(&format!("… nothing for {}s ({state}; {seen} events seen)", QUIET_NOTICE.as_secs()), args.color)
                    );
                }
            }
            _ = tokio::signal::ctrl_c() => break,
        }
    }

    if !args.quiet {
        let breakdown = by_kind
            .iter()
            .map(|(kind, n)| format!("{kind} {n}"))
            .collect::<Vec<_>>()
            .join(", ");
        eprintln!(
            "\n{}",
            dim(
                &format!(
                    "{shown} shown of {seen} received{}",
                    if breakdown.is_empty() {
                        String::new()
                    } else {
                        format!(" — {breakdown}")
                    }
                ),
                args.color
            )
        );
    }
    Ok(())
}

/// `mc-tail <key> --say "..."`: speak one line, confirm, exit. Same path Discord
/// uses, which makes it the quickest way to prove the relay end to end.
async fn say_once(args: &Args, text: &str) -> eyre::Result<()> {
    let Some(server) = args.server.clone() else {
        eyre::bail!("--say needs a server key, e.g. `mc-tail 2b2t --say \"hello\"`");
    };
    let status = status_or_hint(args).await?;
    match status.servers.iter().find(|s| s.server == server) {
        None => eyre::bail!("the gateway is not carrying \"{server}\""),
        Some(row) if row.producers == 0 => {
            eyre::bail!("no logger is connected for \"{server}\", so nothing can say it")
        }
        _ => {}
    }

    let sub = StreamSubscriber::spawn_with(
        args.addr.clone(),
        server.clone(),
        SubscribeOptions {
            replay: 0,
            since: None,
            token: args.token.clone(),
            client: Some("mc-tail".into()),
        },
    );
    // Wait for the hello to be accepted: queueing the line against a connection
    // that has not been established yet means exiting before it is flushed.
    for _ in 0..50 {
        if sub.connected() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !sub.connected() {
        eyre::bail!("could not attach to the gateway at {}", args.addr);
    }

    let from = args.say_as.clone().unwrap_or_else(|| "console".to_string());
    sub.say(text, Some(from.clone()));
    tokio::time::sleep(Duration::from_millis(600)).await;
    eprintln!(
        "{}",
        dim(&format!("sent to {server} as {from}: {text}"), args.color)
    );
    Ok(())
}

async fn status_or_hint(args: &Args) -> eyre::Result<GatewayStatus> {
    fetch_status(&args.addr, args.token.clone())
        .await
        .map_err(|err| {
            eyre::eyre!(
                "cannot reach the gateway at {}: {err}\n\
                 · is it running?   systemctl status terminal-client\n\
                 · right address?   set --addr or STREAM_ADDR (default {DEFAULT_ADDR})\n\
                 · token required?  set --token or {}",
                args.addr,
                mc_stream::TOKEN_ENV
            )
        })
}

// ---------------------------------------------------------------------------
// Filtering
// ---------------------------------------------------------------------------

fn passes(event: &StreamEvent, args: &Args) -> bool {
    if !args.kinds.is_empty() {
        let kind = event.kind().to_lowercase();
        if !args.kinds.iter().any(|want| kind_matches(want, &kind)) {
            return false;
        }
    }
    if !args.players.is_empty() {
        let named = event.names();
        if !args.players.iter().any(|want| {
            named
                .iter()
                .any(|name| name.eq_ignore_ascii_case(want))
        }) {
            return false;
        }
    }
    if let Some(grep) = &args.grep {
        let needle = grep.to_lowercase();
        let hay = format!("{} {}", event.text(), event.player().unwrap_or_default()).to_lowercase();
        if !hay.contains(&needle) {
            return false;
        }
    }
    true
}

/// Kind filters are forgiving: `chat` also matches `c` and `whisper`, `goal`
/// matches `advancement`, and any prefix works.
fn kind_matches(want: &str, kind: &str) -> bool {
    let want = want.trim().to_lowercase();
    if want == kind {
        return true;
    }
    match want.as_str() {
        "chat" | "msg" | "message" => matches!(kind, "chat" | "c" | "whisper" | "w"),
        "goal" | "advancement" | "adv" => matches!(kind, "advancement" | "goal"),
        "kill" | "death" => matches!(kind, "death" | "kill"),
        "system" | "server" => matches!(kind, "server" | "system"),
        _ => kind.starts_with(&want),
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

fn format_event(event: &StreamEvent, args: &Args) -> String {
    let mut out = String::new();
    if let Some(stamp) = timestamp(event.ts(), args.time) {
        out.push_str(&dim(&stamp, args.color));
        out.push(' ');
    }
    if args.show_seq {
        out.push_str(&dim(
            &format!("#{:<8}", event.seq().unwrap_or(0)),
            args.color,
        ));
        out.push(' ');
    }
    // The server drew this line itself — ranks, colours, formatting and all —
    // so print exactly that, and the feed reads like the game's own chat.
    if !args.badges {
        if let Some(ansi) = event.ansi() {
            out.push_str(&if args.color {
                ansi.to_string()
            } else {
                strip_ansi(ansi)
            });
            return out;
        }
        // Tab-list presence has no line behind it. Vanilla prints join and
        // leave in yellow, so match the game rather than inventing a style.
        if let StreamEvent::PlayerEvent(e) = event {
            let verb = match e.event_type.as_str() {
                "j" | "join" => "joined the game",
                "l" | "leave" => "left the game",
                other => other,
            };
            out.push_str(&paint(
                &format!("{} {verb}", e.player_name),
                MC_YELLOW,
                args.color,
            ));
            return out;
        }
    }

    let kind = event.kind();
    out.push_str(&paint(&format!("{:<12}", badge(kind)), kind_color(kind), args.color));
    out.push(' ');

    match event {
        StreamEvent::Chat(c) => {
            let who = c
                .sender_label
                .as_deref()
                .or(c.sender_name.as_deref())
                .or(c.subject_name.as_deref());
            if let Some(who) = who {
                out.push_str(&bold(who, args.color));
                out.push_str("  ");
            }
            out.push_str(&c.plain_text);
        }
        StreamEvent::PlayerEvent(e) => {
            out.push_str(&bold(&e.player_name, args.color));
            out.push_str("  ");
            out.push_str(&e.event_type);
            out.push_str(&dim(&format!(" ({})", e.source), args.color));
        }
    }
    out
}

fn badge(kind: &str) -> String {
    match kind {
        "c" => "chat".to_string(),
        "w" => "whisper".to_string(),
        other => other.to_string(),
    }
}

fn timestamp(ts: DateTime<Utc>, style: TimeStyle) -> Option<String> {
    match style {
        TimeStyle::None => None,
        TimeStyle::Clock => Some(ts.with_timezone(&Local).format("%H:%M:%S").to_string()),
        TimeStyle::Full => Some(
            ts.with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
        ),
        TimeStyle::Rel => {
            let secs = (Utc::now() - ts).num_seconds().max(0);
            Some(match secs {
                0..=59 => format!("{secs:>3}s"),
                60..=3599 => format!("{:>3}m", secs / 60),
                3600..=86_399 => format!("{:>3}h", secs / 3600),
                _ => format!("{:>3}d", secs / 86_400),
            })
        }
    }
}

fn render_status(status: &GatewayStatus, color: bool) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{} on {}\n",
        bold(&status.gateway, color),
        status.listen
    ));
    out.push_str(&dim(
        &format!(
            "up {} · {} connection(s) · buffer {} · auth {} · keys {}\n\n",
            human_secs(status.uptime_secs),
            status.connections,
            status.buffer_cap,
            if status.auth { "required" } else { "open" },
            if status.allowed_keys.is_empty() {
                "any".to_string()
            } else {
                status.allowed_keys.join(", ")
            }
        ),
        color,
    ));

    if status.servers.is_empty() {
        out.push_str("No server keys yet — no logger has connected.\n");
        out.push_str(&dim(
            "Check EVENT_STREAM_ADDR= and SERVER_KEY= in azalea-bot/.env\n",
            color,
        ));
        return out;
    }

    let key_width = status
        .servers
        .iter()
        .map(|s| s.server.len())
        .max()
        .unwrap_or(6)
        .max(6);
    out.push_str(&dim(
        &format!(
            "{:<key_width$}  {:<9}  {:>3} {:>3}  {:>8}  {:>9}  {:>7}  {}\n",
            "SERVER", "STATE", "IN", "OUT", "EV/MIN", "BUFFERED", "DROPPED", "LAST EVENT",
            key_width = key_width
        ),
        color,
    ));
    for server in &status.servers {
        let state = server.health();
        out.push_str(&format!(
            "{:<key_width$}  {}  {:>3} {:>3}  {:>8.1}  {:>9}  {:>7}  {}\n",
            server.server,
            paint(&format!("{state:<9}"), health_color(state), color),
            server.producers,
            server.consumers,
            server.events_per_min,
            server.buffered,
            server.dropped,
            match server.last_event_secs {
                Some(secs) => format!("{} ago", human_secs(secs)),
                None => "never".to_string(),
            },
            key_width = key_width
        ));
    }

    out.push('\n');
    // Who is attached, so "why are there two consumers?" has an answer.
    for server in &status.servers {
        if server.clients.is_empty() {
            continue;
        }
        let who: Vec<String> = server
            .clients
            .iter()
            .map(|c| format!("{} ({}, v{})", c.name, c.role, c.v))
            .collect();
        out.push_str(&dim(
            &format!("{}: {}\n", server.server, who.join(", ")),
            color,
        ));
    }
    let said: u64 = status.servers.iter().map(|s| s.said).sum();
    if said > 0 {
        out.push_str(&dim(
            &format!("{said} line(s) relayed from Discord into the game.\n"),
            color,
        ));
    }
    out.push('\n');
    out.push_str(&dim("IN/OUT are connected producers and consumers.\n", color));
    for server in &status.servers {
        if !server.allowed {
            out.push_str(&warn(
                &format!(
                    "\"{}\" is not in SERVER_KEYS — it is buffered but was never meant to be here.\n",
                    server.server
                ),
                color,
            ));
        }
        if server.malformed > 0 {
            out.push_str(&warn(
                &format!(
                    "\"{}\" sent {} unreadable line(s); the logger may be an older build.\n",
                    server.server, server.malformed
                ),
                color,
            ));
        }
        if server.ring_evicted > 0 {
            out.push_str(&warn(
                &format!(
                    "\"{}\" evicted {} buffered event(s) before any reader saw them — BUFFER may be too small for this key's traffic.\n",
                    server.server, server.ring_evicted
                ),
                color,
            ));
        }
        if server.producers == 0 {
            out.push_str(&warn(
                &format!(
                    "\"{}\" has no logger connected — nothing new will arrive.\n",
                    server.server
                ),
                color,
            ));
        } else if server.consumers == 0 {
            out.push_str(&dim(
                &format!(
                    "\"{}\" has no reader — the Discord bot is not subscribed to it.\n",
                    server.server
                ),
                color,
            ));
        }
    }
    out
}

fn human_secs(secs: u64) -> String {
    match secs {
        0..=59 => format!("{secs}s"),
        60..=3599 => format!("{}m", secs / 60),
        3600..=86_399 => format!("{}h{:02}m", secs / 3600, (secs % 3600) / 60),
        _ => format!("{}d{}h", secs / 86_400, (secs % 86_400) / 3600),
    }
}

const RESET: &str = "\x1b[0m";
/// Minecraft's §e — what vanilla uses for join and leave lines.
const MC_YELLOW: &str = "\x1b[38;2;255;255;85m";

/// Drop ANSI escapes so `--no-color` and piped output stay readable.
fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c != '\x1b' {
            out.push(c);
            continue;
        }
        // ESC '[' params... final. The '[' has to be consumed first: it sits
        // inside @..~ itself, so testing it as the terminator ends the sequence
        // immediately and spills the parameters into the output.
        if chars.next() == Some('[') {
            for next in chars.by_ref() {
                if ('@'..='~').contains(&next) {
                    break;
                }
            }
        }
    }
    out
}

fn paint(text: &str, code: &str, color: bool) -> String {
    if color {
        format!("{code}{text}{RESET}")
    } else {
        text.to_string()
    }
}

fn bold(text: &str, color: bool) -> String {
    paint(text, "\x1b[1m", color)
}

fn dim(text: &str, color: bool) -> String {
    paint(text, "\x1b[2m", color)
}

fn warn(text: &str, color: bool) -> String {
    paint(text, "\x1b[33m", color)
}

fn kind_color(kind: &str) -> &'static str {
    match kind {
        "chat" | "c" => "\x1b[36m",
        "whisper" | "w" => "\x1b[35m",
        "death" | "kill" => "\x1b[31m",
        "join" => "\x1b[32m",
        "leave" => "\x1b[33m",
        "advancement" | "goal" => "\x1b[95m",
        _ => "\x1b[90m",
    }
}

fn health_color(state: &str) -> &'static str {
    match state {
        "live" => "\x1b[32m",
        "quiet" => "\x1b[33m",
        _ => "\x1b[31m",
    }
}

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

/// `Ok(None)` means the command already did its job (help/version).
fn parse_args(argv: Vec<String>) -> eyre::Result<Option<Args>> {
    let mut args = Args::default();
    let mut positional: Vec<String> = Vec::new();
    let mut iter = argv.into_iter();

    while let Some(arg) = iter.next() {
        let value = |flag: &str, iter: &mut std::vec::IntoIter<String>| -> eyre::Result<String> {
            iter.next()
                .ok_or_else(|| eyre::eyre!("{flag} needs a value"))
        };
        match arg.as_str() {
            "-h" | "--help" => {
                print!("{HELP}");
                return Ok(None);
            }
            "-V" | "--version" => {
                println!("mc-tail {}", env!("CARGO_PKG_VERSION"));
                return Ok(None);
            }
            "-a" | "--addr" => args.addr = value("--addr", &mut iter)?,
            "-n" | "--replay" => {
                let raw = value("--replay", &mut iter)?;
                args.replay = raw
                    .parse()
                    .map_err(|_| eyre::eyre!("--replay wants a number, got \"{raw}\""))?;
            }
            "--since" => {
                let raw = value("--since", &mut iter)?;
                args.since = Some(
                    raw.trim_start_matches('#')
                        .parse()
                        .map_err(|_| eyre::eyre!("--since wants a sequence number, got \"{raw}\""))?,
                );
            }
            "-k" | "--kind" | "--kinds" | "-f" | "--filter" => {
                args.kinds.extend(split_list(&value("--kind", &mut iter)?));
            }
            "-p" | "--player" | "--players" => {
                args.players
                    .extend(split_list(&value("--player", &mut iter)?));
            }
            "-g" | "--grep" => args.grep = Some(value("--grep", &mut iter)?),
            "--token" => args.token = Some(value("--token", &mut iter)?),
            "-t" | "--time" => {
                let raw = value("--time", &mut iter)?;
                args.time = match raw.as_str() {
                    "clock" | "time" => TimeStyle::Clock,
                    "rel" | "relative" => TimeStyle::Rel,
                    "full" | "date" => TimeStyle::Full,
                    "none" | "off" => TimeStyle::None,
                    other => eyre::bail!("--time wants clock|rel|full|none, got \"{other}\""),
                };
            }
            "--badges" | "--kinds-column" => args.badges = true,
            "--json" => args.json = true,
            "--seq" => args.show_seq = true,
            "--no-color" | "--no-colour" | "--plain" => args.color = false,
            "--color" | "--colour" => args.color = true,
            "-q" | "--quiet" => args.quiet = true,
            "--say" => args.mode = Mode::Say(value("--say", &mut iter)?),
            "--as" => args.say_as = Some(value("--as", &mut iter)?),
            "-s" | "--status" => args.mode = Mode::Status,
            "-l" | "--list" | "--keys" => args.mode = Mode::List,
            other if other.starts_with('-') && other.len() > 1 => {
                eyre::bail!("unknown option \"{other}\"")
            }
            other => positional.push(other.to_string()),
        }
    }

    // Positionals, tolerating the old `mc-tail ADDR KEY REPLAY` form.
    for arg in positional {
        if looks_like_addr(&arg) {
            args.addr = arg;
        } else if let Ok(replay) = arg.parse::<u32>() {
            args.replay = replay;
        } else {
            args.server = Some(arg);
        }
    }

    if args.since.is_some() {
        args.replay = 0;
    }
    validate_kinds(&args.kinds)?;
    if let Mode::Say(text) = &args.mode {
        if text.trim().is_empty() {
            eyre::bail!("--say needs non-empty text, e.g. `mc-tail 2b2t --say \"hello\"`");
        }
    }
    Ok(Some(args))
}

/// A typo'd `--kind` (e.g. `deth`) used to just match nothing and silently
/// show an empty feed forever, with no hint that the filter was the problem.
const KNOWN_KINDS: &[&str] = &[
    "chat", "whisper", "join", "leave", "death", "kill", "advancement", "server",
];

fn validate_kinds(kinds: &[String]) -> eyre::Result<()> {
    for want in kinds {
        if !KNOWN_KINDS.iter().any(|known| kind_matches(want, known)) {
            eyre::bail!(
                "unknown --kind \"{want}\". Recognized: {} (aliases like msg, goal, adv, system work too, and any prefix matches)",
                KNOWN_KINDS.join(", ")
            );
        }
    }
    Ok(())
}

fn split_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// `host:port` or a bare IP — anything a server key would never be.
fn looks_like_addr(arg: &str) -> bool {
    match arg.rsplit_once(':') {
        Some((host, port)) => !host.is_empty() && port.parse::<u16>().is_ok(),
        None => arg.parse::<std::net::IpAddr>().is_ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(argv: &[&str]) -> Args {
        parse_args(argv.iter().map(|s| s.to_string()).collect())
            .unwrap()
            .unwrap()
    }

    #[test]
    fn legacy_positional_form_still_works() {
        let args = parse(&["127.0.0.1:9700", "ninebninet", "50"]);
        assert_eq!(args.addr, "127.0.0.1:9700");
        assert_eq!(args.server.as_deref(), Some("ninebninet"));
        assert_eq!(args.replay, 50);
    }

    #[test]
    fn key_alone_is_enough() {
        let args = parse(&["ninebninet"]);
        assert_eq!(args.server.as_deref(), Some("ninebninet"));
        assert_eq!(args.replay, DEFAULT_REPLAY);
    }

    #[test]
    fn flags_beat_positionals_and_lists_accumulate() {
        let args = parse(&[
            "2b2t", "-a", "10.0.0.5:9700", "-k", "death,join", "-p", "Notch", "-p", "Herobrine",
        ]);
        assert_eq!(args.addr, "10.0.0.5:9700");
        assert_eq!(args.kinds, vec!["death", "join"]);
        assert_eq!(args.players, vec!["Notch", "Herobrine"]);
    }

    #[test]
    fn since_disables_the_replay_window() {
        let args = parse(&["2b2t", "-n", "100", "--since", "#4200"]);
        assert_eq!(args.since, Some(4200));
        assert_eq!(args.replay, 0);
    }

    #[test]
    fn unknown_options_are_reported_not_swallowed() {
        assert!(parse_args(vec!["--nope".into()]).is_err());
        assert!(parse_args(vec!["--replay".into()]).is_err());
    }

    #[test]
    fn typo_d_kind_is_rejected_instead_of_silently_matching_nothing() {
        assert!(parse_args(vec!["2b2t".into(), "-k".into(), "deth".into()]).is_err());
        assert!(parse_args(vec!["2b2t".into(), "-k".into(), "death".into()]).is_ok());
        assert!(parse_args(vec!["2b2t".into(), "-k".into(), "dea".into()]).is_ok());
    }

    #[test]
    fn say_needs_actual_text() {
        assert!(parse_args(vec!["2b2t".into(), "--say".into(), "  ".into()]).is_err());
        assert!(parse_args(vec!["2b2t".into(), "--say".into(), "hi".into()]).is_ok());
    }

    #[test]
    fn kind_filters_are_forgiving() {
        assert!(kind_matches("chat", "c"));
        assert!(kind_matches("goal", "advancement"));
        assert!(kind_matches("adv", "advancement"));
        assert!(kind_matches("dea", "death"));
        assert!(!kind_matches("join", "leave"));
    }

    #[test]
    fn ansi_escapes_survive_and_can_be_stripped() {
        let coloured = "\x1b[38;2;255;85;85mNotch\x1b[0m fell";
        assert_eq!(strip_ansi(coloured), "Notch fell");
        assert_eq!(strip_ansi("no escapes here"), "no escapes here");
    }

    #[test]
    fn addresses_are_distinguished_from_server_keys() {
        assert!(looks_like_addr("127.0.0.1:9700"));
        assert!(looks_like_addr("gateway.internal:9700"));
        assert!(!looks_like_addr("ninebninet"));
        assert!(!looks_like_addr("2b2t"));
    }

    #[test]
    fn player_filter_matches_any_name_on_the_line() {
        let mut args = Args::default();
        args.players = vec!["herobrine".into()];
        let event = StreamEvent::Chat(mc_stream::ChatEvent {
            seq: Some(1),
            id: None,
            ts: Utc::now(),
            kind: "death".into(),
            sender_name: None,
            sender_label: None,
            subject_name: Some("Notch".into()),
            killer_name: Some("Herobrine".into()),
            content: None,
            plain_text: "Notch was slain by Herobrine".into(),
            ansi: None,
            server_host: None,
        });
        assert!(passes(&event, &args));
        args.players = vec!["someoneelse".into()];
        assert!(!passes(&event, &args));
    }
}
