use std::env;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub key: String,
    pub label: String,
    pub database_url: String,
    /// Optional live event stream (`host:port`) for this server's terminal-client.
    pub stream_addr: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DiscordConfig {
    pub token: String,
    pub client_id: u64,
    pub guild_id: Option<u64>,
    pub bridge_channel_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeStyle {
    Rich,
    Compact,
}

impl BridgeStyle {
    pub fn parse(s: &str) -> Self {
        if s.eq_ignore_ascii_case("compact") {
            Self::Compact
        } else {
            Self::Rich
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rich => "rich",
            Self::Compact => "compact",
        }
    }
}

#[derive(Debug, Clone)]
pub struct BridgeConfig {
    pub kinds: Vec<String>,
    pub poll_ms: u64,
    pub embeds_per_message: usize,
    pub lines_per_message: usize,
    pub max_rows_per_poll: i64,
    pub start_from_latest: bool,
    /// Post a message in the feed channel when the live gateway link comes up,
    /// goes down, or the feed falls back to database polling.
    pub status_notices: bool,
}

#[derive(Debug, Clone)]
pub struct TopicConfig {
    pub enabled: bool,
    pub interval_ms: u64,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub servers: Vec<ServerConfig>,
    pub discord: DiscordConfig,
    pub bridge: BridgeConfig,
    pub topic: TopicConfig,
    pub head_size: u32,
}

impl Config {
    pub fn load() -> eyre::Result<Self> {
        let _ = dotenvy::dotenv();
        let servers = load_servers()?;
        let guild_raw = optional("DISCORD_GUILD_ID", "");
        let guild_id = if guild_raw.is_empty() {
            None
        } else {
            Some(guild_raw.parse()?)
        };
        let bridge_ch = optional("BRIDGE_CHANNEL_ID", "");
        Ok(Self {
            discord: DiscordConfig {
                token: required("DISCORD_TOKEN")?,
                client_id: required("DISCORD_CLIENT_ID")?.parse()?,
                guild_id,
                bridge_channel_id: if bridge_ch.is_empty() {
                    None
                } else {
                    Some(bridge_ch)
                },
            },
            bridge: BridgeConfig {
                kinds: list("BRIDGE_KINDS", "chat,join,leave,death,advancement,server"),
                poll_ms: bounded("BRIDGE_POLL_MS", 3_000, 1_000, 300_000),
                embeds_per_message: bounded("BRIDGE_EMBEDS_PER_MESSAGE", 10, 1, 10) as usize,
                lines_per_message: bounded("BRIDGE_LINES_PER_MESSAGE", 20, 1, 60) as usize,
                max_rows_per_poll: bounded("BRIDGE_MAX_ROWS_PER_POLL", 40, 1, 500) as i64,
                start_from_latest: optional("BRIDGE_START_FROM_LATEST", "true") != "false",
                status_notices: optional("BRIDGE_STATUS_NOTICES", "true") != "false",
            },
            topic: TopicConfig {
                enabled: optional("TOPIC_UPDATES", "true") != "false",
                // Default is now the floor: Discord rate-limits channel
                // name/topic edits to about 2 per 10 minutes per channel, so
                // 300_000ms (5 min) is already the fastest this can safely
                // run without edits starting to get rejected/rate-limited.
                interval_ms: bounded("TOPIC_INTERVAL_MS", 300_000, 300_000, 86_400_000),
            },
            head_size: num("HEAD_SIZE", 64) as u32,
            servers,
        })
    }

    pub fn head_url_name(&self, name: &str) -> String {
        head_url_for_name(name, self.head_size)
    }

    /// Full body render for the embed thumbnail.
    pub fn body_url_name(&self, name: &str) -> String {
        body_url_for_name(name)
    }

    pub fn server_by_key(&self, key: &str) -> Option<&ServerConfig> {
        self.servers.iter().find(|s| s.key == key)
    }

    pub fn multi_server(&self) -> bool {
        self.servers.len() > 1
    }
}

fn required(name: &str) -> eyre::Result<String> {
    let value = env::var(name).unwrap_or_default();
    let trimmed = value.trim();
    if trimmed.is_empty() {
        eyre::bail!(
            "Missing required environment variable {name}. Copy .env.example to .env and fill it in."
        );
    }
    Ok(trimmed.to_string())
}

fn optional(name: &str, fallback: &str) -> String {
    match env::var(name) {
        Ok(v) if !v.trim().is_empty() => v.trim().to_string(),
        _ => fallback.to_string(),
    }
}

fn num(name: &str, fallback: u64) -> u64 {
    match env::var(name) {
        Ok(raw) if !raw.trim().is_empty() => raw.trim().parse().unwrap_or_else(|_| {
            panic!("Environment variable {name} must be a number, got \"{raw}\"")
        }),
        _ => fallback,
    }
}

fn bounded(name: &str, fallback: u64, min: u64, max: u64) -> u64 {
    num(name, fallback).clamp(min, max)
}

fn list(name: &str, fallback: &str) -> Vec<String> {
    optional(name, fallback)
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}

fn load_stream_addr(per_server: &str) -> Option<String> {
    let dedicated = optional(per_server, "");
    if !dedicated.is_empty() {
        return Some(dedicated);
    }
    let shared = optional("EVENT_STREAM_ADDR", "");
    if shared.is_empty() {
        None
    } else {
        Some(shared)
    }
}

fn load_servers() -> eyre::Result<Vec<ServerConfig>> {
    let keys = list("SERVERS", "");
    if keys.is_empty() {
        return Ok(vec![ServerConfig {
            key: "default".into(),
            label: optional("SERVER_LABEL", "Minecraft"),
            database_url: required("DATABASE_URL")?,
            stream_addr: load_stream_addr("EVENT_STREAM_ADDR"),
        }]);
    }
    keys.into_iter()
        .map(|key| {
            let upper = key
                .to_uppercase()
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                .collect::<String>();
            let url_key = format!("SERVER_{upper}_URL");
            let url = env::var(&url_key).unwrap_or_default();
            let url = url.trim();
            if url.is_empty() {
                eyre::bail!("SERVERS lists \"{key}\" but {url_key} is not set.");
            }
            Ok(ServerConfig {
                key: key.clone(),
                label: optional(&format!("SERVER_{upper}_LABEL"), &key),
                database_url: url.to_string(),
                stream_addr: load_stream_addr(&format!("SERVER_{upper}_STREAM")),
            })
        })
        .collect()
}

/// Player renders, addressed by name.
///
/// crafthead and mc-heads both render straight from a bare username too — no
/// uuid resolution needed, so no lookup or cache stands between a name and a
/// render URL. For a cracked account sharing a name with someone else's real
/// account this can render the wrong skin; that trade was made deliberately
/// in favor of instant, uuid-lookup-free renders.
pub fn head_url_for_name(name: &str, size: u32) -> String {
    format!("https://crafthead.net/helm/{name}/{size}")
}

/// Full body render, for the larger embed thumbnail.
pub fn body_url_for_name(name: &str) -> String {
    format!("https://mc-heads.net/body/{name}")
}
