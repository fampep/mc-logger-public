//! One-shot Discord channel export over the official bot HTTP API.
//!
//! Does **not** open a gateway session — safe to run while `mc-discord-bot.service`
//! is already logged in as 0boz. Bot token only (no user/self-bot tokens).
//!
//! Usage (from `mc-discord-bot/`, token from `.env` or `DISCORD_TOKEN`):
//!   cargo run --release --bin discord-scrape -- --channel CHANNEL_ID
//!   cargo run --release --bin discord-scrape -- --channel CHANNEL_ID --out log.jsonl
//!   cargo run --release --bin discord-scrape -- --channel CHANNEL_ID --limit 500 --format text
//!
//! Flags: --channel/-c, --token, --out/-o (default ./channel-<id>.jsonl; - = stdout),
//!        --limit, --after, --before, --format jsonl|text
//! Env:   DISCORD_TOKEN, CHANNEL_ID
//!
//! Needs View Channel + Read Message History on that channel. Does not export DMs.

use std::env;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Serialize;
use serenity::builder::GetMessages;
use serenity::http::{Http, HttpError};
use serenity::model::channel::{Channel, ChannelType, Message};
use serenity::model::id::{ChannelId, MessageId};

const HELP: &str = "\
discord-scrape — export a Discord guild channel via the official bot HTTP API.

USAGE:
  discord-scrape --channel <CHANNEL_ID> [options]

OPTIONS:
  --channel, -c <ID>   Channel snowflake (or env CHANNEL_ID)
  --token <TOKEN>      Bot token (or env DISCORD_TOKEN / mc-discord-bot/.env)
  --out, -o <PATH>     Output file (default: ./channel-<id>.jsonl; - for stdout)
  --limit <N>          Max messages (default: all, paginated 100/request)
  --after <ID>         Only messages newer than this snowflake
  --before <ID>        Only messages older than this snowflake
  --format <fmt>       jsonl (default) or text
  -h, --help           Show this help

Token is never printed. This process does not connect to the Discord gateway.
";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Format {
    Jsonl,
    Text,
}

struct Args {
    channel_id: u64,
    token: String,
    out: Option<PathBuf>,
    stdout: bool,
    limit: Option<u64>,
    after: Option<u64>,
    before: Option<u64>,
    format: Format,
}

#[derive(Serialize)]
struct ExportedAuthor {
    id: String,
    name: String,
    bot: bool,
}

#[derive(Serialize)]
struct ExportedAttachment {
    url: String,
    filename: String,
}

#[derive(Serialize)]
struct ExportedMessage {
    id: String,
    timestamp: String,
    author: ExportedAuthor,
    content: String,
    attachments: Vec<ExportedAttachment>,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}

async fn run() -> eyre::Result<()> {
    load_env();
    let args = parse_args()?;

    if args.token.trim().starts_with("Bearer ") {
        eyre::bail!(
            "This tool only accepts a Discord bot token (DISCORD_TOKEN), not a user/Bearer token."
        );
    }

    let http = Http::new(args.token.trim());
    let me = match http.get_current_user().await {
        Ok(user) => user,
        Err(err) => eyre::bail!("{}", describe_http_error(&err)),
    };
    if !me.bot {
        eyre::bail!(
            "The token belongs to a user account, not a bot. Use DISCORD_TOKEN for the 0boz bot."
        );
    }
    eprintln!("logged in as {} (bot, HTTP only — no gateway)", me.tag());

    let channel_id = ChannelId::new(args.channel_id);
    let channel = match http.get_channel(channel_id).await {
        Ok(ch) => ch,
        Err(err) => eyre::bail!("{}", describe_http_error(&err)),
    };
    let label = describe_channel(&channel)?;
    eprintln!("exporting {label}");

    let messages = fetch_messages(&http, channel_id, &args).await?;
    if messages.is_empty() {
        eprintln!(
            "no messages returned. The channel may be empty, or the bot is missing \
             Read Message History (Discord returns an empty list instead of 403 in that case)."
        );
    } else {
        eprintln!("fetched {} message(s)", messages.len());
    }

    let exported: Vec<ExportedMessage> = messages.iter().map(export_message).collect();
    write_output(&args, &exported)?;
    Ok(())
}

fn load_env() {
    let manifest_env = Path::new(env!("CARGO_MANIFEST_DIR")).join(".env");
    let _ = dotenvy::dotenv();
    let _ = dotenvy::from_path(&manifest_env);
}

fn parse_args() -> eyre::Result<Args> {
    let raw: Vec<String> = env::args().skip(1).collect();
    if raw.iter().any(|a| a == "-h" || a == "--help") {
        print!("{HELP}");
        std::process::exit(0);
    }

    let mut channel: Option<String> = env::var("CHANNEL_ID").ok().filter(|s| !s.is_empty());
    let mut token: Option<String> = None;
    let mut out: Option<String> = None;
    let mut limit: Option<u64> = None;
    let mut after: Option<u64> = None;
    let mut before: Option<u64> = None;
    let mut format = Format::Jsonl;
    let mut i = 0;
    while i < raw.len() {
        let arg = raw[i].as_str();
        let (key, inline) = split_flag(arg);
        match key {
            "--channel" | "-c" => {
                channel = Some(need_value(key, inline, &raw, &mut i)?);
            }
            "--token" => {
                token = Some(need_value(key, inline, &raw, &mut i)?);
            }
            "--out" | "-o" => {
                out = Some(need_value(key, inline, &raw, &mut i)?);
            }
            "--limit" => {
                let v = need_value(key, inline, &raw, &mut i)?;
                limit = Some(parse_positive(&v, "--limit")?);
            }
            "--after" => {
                let v = need_value(key, inline, &raw, &mut i)?;
                after = Some(parse_snowflake(&v, "--after")?);
            }
            "--before" => {
                let v = need_value(key, inline, &raw, &mut i)?;
                before = Some(parse_snowflake(&v, "--before")?);
            }
            "--format" => {
                let v = need_value(key, inline, &raw, &mut i)?;
                format = match v.as_str() {
                    "jsonl" | "json" => Format::Jsonl,
                    "text" | "txt" => Format::Text,
                    other => eyre::bail!("--format must be jsonl or text, got {other:?}"),
                };
            }
            other if other.starts_with('-') => {
                eyre::bail!("unknown flag {other}. Use --help.");
            }
            other => {
                if channel.is_none() {
                    channel = Some(other.to_string());
                } else {
                    eyre::bail!("unexpected argument {other:?}. Use --help.");
                }
            }
        }
        i += 1;
    }

    let channel_id = match channel {
        Some(id) => parse_snowflake(&id, "--channel")?,
        None => eyre::bail!("missing --channel CHANNEL_ID (or env CHANNEL_ID). Use --help."),
    };

    let token = token
        .or_else(|| env::var("DISCORD_TOKEN").ok().filter(|s| !s.is_empty()))
        .ok_or_else(|| {
            eyre::eyre!(
                "missing bot token. Set DISCORD_TOKEN in mc-discord-bot/.env or pass --token."
            )
        })?;

    if let (Some(a), Some(b)) = (after, before) {
        if a >= b {
            eyre::bail!("--after must be less than --before");
        }
    }

    let (stdout, out_path) = match out.as_deref() {
        Some("-") => (true, None),
        Some(path) => (false, Some(PathBuf::from(path))),
        None => {
            let ext = match format {
                Format::Jsonl => "jsonl",
                Format::Text => "txt",
            };
            (
                false,
                Some(PathBuf::from(format!("channel-{channel_id}.{ext}"))),
            )
        }
    };

    Ok(Args {
        channel_id,
        token,
        out: out_path,
        stdout,
        limit,
        after,
        before,
        format,
    })
}

fn split_flag(arg: &str) -> (&str, Option<&str>) {
    if let Some((k, v)) = arg.split_once('=') {
        (k, Some(v))
    } else {
        (arg, None)
    }
}

fn need_value(
    flag: &str,
    inline: Option<&str>,
    raw: &[String],
    i: &mut usize,
) -> eyre::Result<String> {
    if let Some(v) = inline {
        if v.is_empty() {
            eyre::bail!("{flag} needs a value");
        }
        return Ok(v.to_string());
    }
    let next = raw
        .get(*i + 1)
        .cloned()
        .ok_or_else(|| eyre::eyre!("{flag} needs a value"))?;
    if next.starts_with('-') && next != "-" {
        eyre::bail!("{flag} needs a value");
    }
    *i += 1;
    Ok(next)
}

fn parse_snowflake(raw: &str, flag: &str) -> eyre::Result<u64> {
    let id: u64 = raw
        .trim()
        .parse()
        .map_err(|_| eyre::eyre!("{flag} must be a Discord snowflake (numeric ID), got {raw:?}"))?;
    if id == 0 {
        eyre::bail!("{flag} must be a non-zero snowflake");
    }
    Ok(id)
}

fn parse_positive(raw: &str, flag: &str) -> eyre::Result<u64> {
    let n: u64 = raw
        .trim()
        .parse()
        .map_err(|_| eyre::eyre!("{flag} must be a positive integer, got {raw:?}"))?;
    if n == 0 {
        eyre::bail!("{flag} must be greater than 0");
    }
    Ok(n)
}

fn describe_channel(channel: &Channel) -> eyre::Result<String> {
    match channel {
        Channel::Private(_) => {
            eyre::bail!("refusing to export a DM channel")
        }
        Channel::Guild(gc) => {
            if matches!(gc.kind, ChannelType::Private | ChannelType::GroupDm) {
                eyre::bail!("refusing to export a DM / group-DM channel");
            }
            Ok(format!("#{} ({}) [{}]", gc.name, gc.id, gc.kind.name()))
        }
        _ => eyre::bail!("unknown channel type — not exporting"),
    }
}

async fn fetch_messages(
    http: &Http,
    channel_id: ChannelId,
    args: &Args,
) -> eyre::Result<Vec<Message>> {
    let mut out = Vec::new();
    let page_cap: u8 = 100;
    // Forward from --after when it is set: Discord `after` returns the next
    // (newer) messages, newest-first within the page. Otherwise walk backward
    // from newest / --before.
    let forward = args.after.is_some();
    let mut cursor_after = args.after;
    let mut cursor_before = if forward { None } else { args.before };
    let mut last_report = 0usize;

    loop {
        if args.limit.is_some_and(|n| out.len() as u64 >= n) {
            break;
        }
        let remaining = args
            .limit
            .map(|n| n.saturating_sub(out.len() as u64))
            .unwrap_or(u64::from(page_cap));
        let take = remaining.min(u64::from(page_cap)) as u8;

        let mut builder = GetMessages::new().limit(take);
        if forward {
            if let Some(id) = cursor_after {
                builder = builder.after(MessageId::new(id));
            }
        } else if let Some(id) = cursor_before {
            builder = builder.before(MessageId::new(id));
        }

        let page = get_messages_retry(http, channel_id, builder).await?;
        if page.is_empty() {
            break;
        }

        let newest_id = page.first().map(|m| m.id.get());
        let oldest_id = page.last().map(|m| m.id.get());
        let page_len = page.len();

        if forward {
            // Page is newest-first; reverse so we append chronological.
            for msg in page.into_iter().rev() {
                let id = msg.id.get();
                if args.before.is_some_and(|b| id >= b) {
                    continue;
                }
                if args.after.is_some_and(|a| id <= a) {
                    continue;
                }
                out.push(msg);
                if args.limit.is_some_and(|n| out.len() as u64 >= n) {
                    break;
                }
            }
            let Some(newest) = newest_id else { break };
            if args.before.is_some_and(|b| newest >= b) {
                break;
            }
            cursor_after = Some(newest);
        } else {
            let mut hit_floor = false;
            for msg in page {
                let id = msg.id.get();
                if args.after.is_some_and(|a| id <= a) {
                    hit_floor = true;
                    break;
                }
                out.push(msg);
                if args.limit.is_some_and(|n| out.len() as u64 >= n) {
                    break;
                }
            }
            if hit_floor {
                break;
            }
            cursor_before = oldest_id;
        }

        if page_len < take as usize {
            break;
        }
        if out.len() >= last_report + 500 {
            last_report = out.len();
            eprintln!("... {} message(s)", out.len());
        }
    }

    if !forward {
        out.reverse();
    }
    if let Some(n) = args.limit {
        out.truncate(n as usize);
    }
    Ok(out)
}

async fn get_messages_retry(
    http: &Http,
    channel_id: ChannelId,
    builder: GetMessages,
) -> eyre::Result<Vec<Message>> {
    let mut backoff = Duration::from_secs(2);
    loop {
        match channel_id.messages(http, builder).await {
            Ok(page) => return Ok(page),
            Err(err) if is_429(&err) => {
                let wait = retry_after(&err).unwrap_or(backoff);
                eprintln!(
                    "rate limited (429), sleeping {}s",
                    wait.as_secs_f32().ceil() as u64
                );
                tokio::time::sleep(wait).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
            Err(err) => eyre::bail!("{}", describe_http_error(&err)),
        }
    }
}

fn is_429(err: &serenity::Error) -> bool {
    matches!(
        err,
        serenity::Error::Http(HttpError::UnsuccessfulRequest(resp)) if resp.status_code.as_u16() == 429
    )
}

fn retry_after(err: &serenity::Error) -> Option<Duration> {
    let serenity::Error::Http(HttpError::UnsuccessfulRequest(resp)) = err else {
        return None;
    };
    // Discord's 429 JSON uses retry_after seconds; serenity may only keep `message`.
    let msg = &resp.error.message;
    if let Some(secs) = msg
        .split_whitespace()
        .filter_map(|w| w.parse::<f64>().ok())
        .find(|n| *n > 0.0 && *n < 3600.0)
    {
        return Some(Duration::from_secs_f64(secs));
    }
    None
}

fn describe_http_error(err: &serenity::Error) -> String {
    let serenity::Error::Http(http_err) = err else {
        return err.to_string();
    };
    let HttpError::UnsuccessfulRequest(resp) = http_err else {
        return http_err.to_string();
    };
    let status = resp.status_code.as_u16();
    let code = resp.error.code;
    let msg = resp.error.message.trim();
    match (status, code) {
        (401, _) => "unauthorized (401): invalid or missing bot token. Check DISCORD_TOKEN.".into(),
        (403, _) | (_, 50_001) | (_, 50_013) => format!(
            "missing permission (HTTP {status}, Discord {code}): the bot needs View Channel \
             and Read Message History on this channel. {msg}"
        ),
        (404, _) | (_, 10_003) => format!(
            "unknown channel (HTTP {status}, Discord {code}): wrong ID, or the bot is not \
             in that server. {msg}"
        ),
        (429, _) => format!("rate limited (429): {msg}. Wait and retry."),
        _ => format!("Discord HTTP {status} (code {code}): {msg}"),
    }
}

fn export_message(msg: &Message) -> ExportedMessage {
    ExportedMessage {
        id: msg.id.get().to_string(),
        timestamp: msg
            .timestamp
            .to_rfc3339()
            .unwrap_or_else(|| msg.timestamp.to_string()),
        author: ExportedAuthor {
            id: msg.author.id.get().to_string(),
            name: msg.author.display_name().to_string(),
            bot: msg.author.bot,
        },
        content: msg.content.clone(),
        attachments: msg
            .attachments
            .iter()
            .map(|a| ExportedAttachment {
                url: a.url.clone(),
                filename: a.filename.clone(),
            })
            .collect(),
    }
}

fn write_output(args: &Args, messages: &[ExportedMessage]) -> eyre::Result<()> {
    if args.stdout {
        let stdout = io::stdout();
        let mut w = BufWriter::new(stdout.lock());
        write_messages(&mut w, args.format, messages)?;
        w.flush()?;
        return Ok(());
    }
    let path = args.out.as_ref().expect("file output path");
    let file = std::fs::File::create(path)?;
    let mut w = BufWriter::new(file);
    write_messages(&mut w, args.format, messages)?;
    w.flush()?;
    eprintln!("wrote {} message(s) to {}", messages.len(), path.display());
    Ok(())
}

fn write_messages<W: Write>(
    w: &mut W,
    format: Format,
    messages: &[ExportedMessage],
) -> eyre::Result<()> {
    match format {
        Format::Jsonl => {
            for msg in messages {
                serde_json::to_writer(&mut *w, msg)?;
                w.write_all(b"\n")?;
            }
        }
        Format::Text => {
            for msg in messages {
                writeln!(
                    w,
                    "[{}] {}: {}",
                    msg.timestamp, msg.author.name, msg.content
                )?;
                for att in &msg.attachments {
                    writeln!(w, "  attachment: {} ({})", att.url, att.filename)?;
                }
            }
        }
    }
    Ok(())
}
