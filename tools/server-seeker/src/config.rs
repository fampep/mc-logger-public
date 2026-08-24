use anyhow::{Result, bail};
use clap::ValueEnum;
use std::env;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq, Default)]
pub enum Mode {
    /// Masscan port scan → status ping on open 25565 (ServerSeekerV2-style)
    #[default]
    Scanner,
    /// Re-ping servers already stored in the database
    Rescanner,
    /// Direct CIDR/IP status ping without masscan (small ranges / testing)
    Cidr,
    /// Continuous loop: masscan → domains → rescan → sleep
    Watch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RescanPriority {
    OldestFirst,
    NewestFirst,
}

impl RescanPriority {
    pub fn from_env() -> Self {
        match env::var("SEEKER_RESCAN_PRIORITY")
            .unwrap_or_else(|_| "oldest".into())
            .to_lowercase()
            .as_str()
        {
            "newest" => Self::NewestFirst,
            _ => Self::OldestFirst,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EnvScanConfig {
    pub database_url: Option<String>,
    pub sqlite_path: Option<String>,
    pub mode: Mode,
    pub queries: Vec<String>,
    pub seeds: Vec<String>,
    pub cidrs: Vec<String>,
    pub port: u16,
    pub concurrency: usize,
    pub timeout: Duration,
    pub rate: u32,
    pub limit: Option<usize>,
    pub watch_interval: Option<Duration>,
    pub i_understand: bool,
    pub masscan_config: Option<String>,
    pub masscan_bin: String,
    pub masscan_sudo: bool,
    pub rescan_priority: RescanPriority,
    pub rescan_limit: Option<usize>,
}

pub fn load_env_config() -> Result<EnvScanConfig> {
    dotenvy::dotenv().ok();

    let mode = env::var("SEEKER_MODE")
        .ok()
        .and_then(|v| parse_mode(&v))
        .unwrap_or(Mode::Watch);

    let watch_secs = env::var("SEEKER_WATCH_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .or_else(|| {
            env::var("SEEKER_WATCH_INTERVAL")
                .ok()
                .and_then(|v| v.parse().ok())
        });

    let mut seeds = split_env("SEEKER_SEEDS");
    seeds.extend(split_env("SEEKER_DOMAINS"));
    seeds.sort();
    seeds.dedup();

    Ok(EnvScanConfig {
        database_url: env::var("SEEKER_DATABASE_URL")
            .ok()
            .or_else(|| env::var("DATABASE_URL").ok())
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty()),
        sqlite_path: env::var("SEEKER_SQLITE_PATH").ok().filter(|v| !v.is_empty()),
        mode,
        queries: split_env("SEEKER_QUERIES"),
        seeds,
        cidrs: split_env("SEEKER_CIDRS"),
        port: env_u16("SEEKER_PORT", 25565),
        concurrency: env_usize("SEEKER_CONCURRENCY", 64),
        timeout: Duration::from_millis(env_u64("SEEKER_TIMEOUT_MS", 4000)),
        rate: env_u32("SEEKER_RATE", 200),
        limit: env::var("SEEKER_LIMIT")
            .ok()
            .and_then(|v| v.parse().ok()),
        watch_interval: watch_secs.map(Duration::from_secs),
        i_understand: truthy("SEEKER_I_UNDERSTAND"),
        masscan_config: env::var("SEEKER_MASSCAN_CONFIG")
            .ok()
            .filter(|v| !v.is_empty()),
        masscan_bin: env::var("SEEKER_MASSCAN_BIN").unwrap_or_else(|_| "masscan".into()),
        masscan_sudo: truthy("SEEKER_MASSCAN_SUDO"),
        rescan_priority: RescanPriority::from_env(),
        rescan_limit: env::var("SEEKER_RESCAN_LIMIT")
            .ok()
            .and_then(|v| v.parse().ok()),
    })
}

pub fn merge_cli_env(
    cli_queries: &[String],
    cli_seeds: &[String],
    cli_cidrs: &[String],
    env_cfg: &EnvScanConfig,
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let queries = if cli_queries.is_empty() {
        env_cfg.queries.clone()
    } else {
        cli_queries.to_vec()
    };
    let mut seeds = if cli_seeds.is_empty() {
        env_cfg.seeds.clone()
    } else {
        cli_seeds.to_vec()
    };
    seeds.sort();
    seeds.dedup();
    let cidrs = if cli_cidrs.is_empty() {
        env_cfg.cidrs.clone()
    } else {
        cli_cidrs.to_vec()
    };
    (queries, seeds, cidrs)
}

pub fn validate_config(
    mode: Mode,
    seeds: &[String],
    cidrs: &[String],
    masscan_config: Option<&str>,
) -> Result<()> {
    match mode {
        Mode::Scanner => {
            if masscan_config.is_none() {
                bail!(
                    "scanner mode requires a masscan config (SEEKER_MASSCAN_CONFIG or --masscan-config)"
                );
            }
        }
        Mode::Cidr => {
            if cidrs.is_empty() {
                bail!("cidr mode requires --cidr or SEEKER_CIDRS");
            }
        }
        Mode::Watch => {
            if masscan_config.is_none() && seeds.is_empty() && cidrs.is_empty() {
                bail!(
                    "watch mode needs at least one of: SEEKER_MASSCAN_CONFIG, SEEKER_SEEDS/SEEKER_DOMAINS, or SEEKER_CIDRS"
                );
            }
        }
        Mode::Rescanner => {}
    }
    Ok(())
}

pub fn default_masscan_config_path() -> PathBuf {
    PathBuf::from("masscan.conf")
}

pub fn default_watch_interval() -> Duration {
    Duration::from_secs(3600)
}

fn parse_mode(s: &str) -> Option<Mode> {
    match s.to_lowercase().as_str() {
        "scanner" | "scan" | "discovery" => Some(Mode::Scanner),
        "rescanner" | "rescan" => Some(Mode::Rescanner),
        "cidr" | "direct" => Some(Mode::Cidr),
        "watch" | "auto" | "live" => Some(Mode::Watch),
        _ => None,
    }
}

pub fn scan_source_label(mode: Mode, detail: Option<&str>) -> String {
    match mode {
        Mode::Scanner => "masscan".to_string(),
        Mode::Rescanner => "rescanner".to_string(),
        Mode::Cidr => detail
            .map(|c| format!("cidr:{c}"))
            .unwrap_or_else(|| "cidr".to_string()),
        Mode::Watch => detail.unwrap_or("watch").to_string(),
    }
}

fn split_env(name: &str) -> Vec<String> {
    env::var(name)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

fn env_u16(name: &str, fallback: u16) -> u16 {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(fallback)
}

fn env_u32(name: &str, fallback: u32) -> u32 {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(fallback)
}

fn env_u64(name: &str, fallback: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(fallback)
}

fn env_usize(name: &str, fallback: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(fallback)
}

fn truthy(name: &str) -> bool {
    matches!(
        env::var(name).ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_prefers_cli() {
        let env_cfg = EnvScanConfig {
            database_url: None,
            sqlite_path: None,
            mode: Mode::Watch,
            queries: vec!["vanilla".into()],
            seeds: vec!["example.com".into()],
            cidrs: vec!["10.0.0.0/8".into()],
            port: 25565,
            concurrency: 64,
            timeout: Duration::from_secs(4),
            rate: 200,
            limit: None,
            watch_interval: Some(Duration::from_secs(3600)),
            i_understand: false,
            masscan_config: Some("masscan.conf".into()),
            masscan_bin: "masscan".into(),
            masscan_sudo: true,
            rescan_priority: RescanPriority::OldestFirst,
            rescan_limit: None,
        };
        let (q, s, c) = merge_cli_env(
            &["cli".into()],
            &["play.example.com".into()],
            &["192.168.0.0/24".into()],
            &env_cfg,
        );
        assert_eq!(q, vec!["cli"]);
        assert_eq!(s, vec!["play.example.com"]);
        assert_eq!(c, vec!["192.168.0.0/24"]);
    }

    #[test]
    fn parses_mode_aliases() {
        assert_eq!(parse_mode("scanner"), Some(Mode::Scanner));
        assert_eq!(parse_mode("rescan"), Some(Mode::Rescanner));
        assert_eq!(parse_mode("cidr"), Some(Mode::Cidr));
        assert_eq!(parse_mode("watch"), Some(Mode::Watch));
        assert_eq!(parse_mode("live"), Some(Mode::Watch));
    }
}
