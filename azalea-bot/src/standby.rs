//! Hot-standby coordination between two loggers on the same server.
//!
//! A standby account sits in-game next to the primary and writes nothing while
//! the primary is healthy. When the primary is kicked, crashes, or its host
//! goes away, the standby starts logging within `STANDBY_TAKEOVER_SECS` and
//! stands down again when the primary comes back.
//!
//! Liveness is a heartbeat row, not "did we see a write recently": on a quiet
//! server those look identical, and guessing wrong means either a gap in the
//! log or two loggers writing every line twice.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::db::Db;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Primary,
    Standby,
}

impl Role {
    /// `LOGGER_ROLE=standby|backup|secondary` — anything else is a primary.
    pub fn from_env(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "standby" | "backup" | "secondary" | "fallback" => Role::Standby,
            _ => Role::Primary,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Role::Primary => "primary",
            Role::Standby => "standby",
        }
    }

    pub fn is_standby(self) -> bool {
        self == Role::Standby
    }
}

/// What the heartbeat task needs to know about the live client each tick.
pub struct Snapshot {
    pub connected: bool,
    pub session_id: i64,
    pub bot_username: Option<String>,
}

pub struct Config {
    /// Unique name for this logger process (`6b6t`, `6b6t-backup`).
    pub instance: String,
    pub role: Role,
    pub host: String,
    /// How long a primary's heartbeat may be stale before a standby takes over.
    pub takeover_after: Duration,
    /// How often to write our own heartbeat and re-check the primary.
    pub interval: Duration,
}

/// Runs the heartbeat + takeover loop for as long as the process lives.
pub fn spawn<F>(db: Arc<Db>, cfg: Config, gate: Arc<AtomicBool>, snapshot: F)
where
    F: Fn() -> Snapshot + Send + 'static,
{
    tokio::spawn(async move {
        let takeover_secs = cfg.takeover_after.as_secs() as i64;
        let mut ticker = tokio::time::interval(cfg.interval);
        // A blip talking to Postgres must not flip the gate; only a definite
        // answer changes who is logging.
        loop {
            ticker.tick().await;
            let snap = snapshot();
            let mut writing = gate.load(Ordering::Acquire);

            if cfg.role.is_standby() {
                match db
                    .primary_is_live(&cfg.host, &cfg.instance, takeover_secs)
                    .await
                {
                    Ok(primary) => {
                        let should_write = snap.connected && primary.is_none();
                        if should_write != writing {
                            gate.store(should_write, Ordering::Release);
                            writing = should_write;
                            if should_write {
                                crate::log(&format!(
                                    "standby taking over: no primary heartbeat for {takeover_secs}s — logging as {}",
                                    snap.bot_username.as_deref().unwrap_or("the standby account")
                                ));
                            } else if let Some(primary) = &primary {
                                crate::log(&format!(
                                    "standby standing down: primary \"{primary}\" is logging again"
                                ));
                            } else {
                                crate::log("standby offline — not logging");
                            }
                        }
                    }
                    Err(error) => {
                        eprintln!("standby check failed (keeping current state): {error}");
                    }
                }
            }

            if let Err(error) = db
                .heartbeat(
                    &cfg.instance,
                    cfg.role.as_str(),
                    &cfg.host,
                    snap.session_id,
                    snap.bot_username.as_deref(),
                    snap.connected,
                    writing,
                )
                .await
            {
                eprintln!("heartbeat failed: {error}");
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_parsing_accepts_the_words_people_use() {
        assert!(Role::from_env("standby").is_standby());
        assert!(Role::from_env("Backup").is_standby());
        assert!(Role::from_env(" fallback ").is_standby());
        assert!(!Role::from_env("primary").is_standby());
        assert!(!Role::from_env("").is_standby());
    }
}
