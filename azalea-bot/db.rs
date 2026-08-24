//! Postgres writer.
//!
//! Writes go through an unbounded channel to a background task that batches
//! them, so a busy server's chat never blocks the bot's event loop. The
//! mineflayer version did the same thing for the same reason: DonutSMP pushed
//! over 150 messages in 20 seconds.

use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

const FLUSH_INTERVAL: Duration = Duration::from_millis(1000);
const FLUSH_MAX_ROWS: usize = 200;

#[derive(Debug)]
pub struct ChatRow {
    pub session_id: i64,
    pub received_at: DateTime<Utc>,
    pub source: String,
    pub kind: String,
    pub sender_name: Option<String>,
    pub sender_uuid: Option<Uuid>,
    /// Who the line is about when there is no sender (joined/left/died).
    pub subject_name: Option<String>,
    /// Who did the killing, for deaths that name one.
    pub killer_name: Option<String>,
    pub content: Option<String>,
    pub plain_text: String,
    pub ansi: Option<String>,
    pub translate_key: Option<String>,
    pub raw_json: Option<serde_json::Value>,
}

#[derive(Debug)]
pub struct PlayerEventRow {
    pub session_id: i64,
    pub server_host: String,
    pub occurred_at: DateTime<Utc>,
    pub event_type: String,
    pub source: String,
    pub player_name: String,
    pub player_uuid: Option<Uuid>,
    pub raw_text: Option<String>,
}

#[derive(Debug)]
pub enum Write {
    Chat(Box<ChatRow>),
    Event(Box<PlayerEventRow>),
}

#[derive(Clone)]
pub struct Writer {
    tx: mpsc::UnboundedSender<Write>,
}

impl Writer {
    pub fn chat(&self, row: ChatRow) {
        let _ = self.tx.send(Write::Chat(Box::new(row)));
    }

    pub fn event(&self, row: PlayerEventRow) {
        let _ = self.tx.send(Write::Event(Box::new(row)));
    }
}

pub struct Db {
    client: Client,
}

impl Db {
    pub async fn connect(url: &str) -> eyre::Result<Self> {
        let (client, connection) = tokio_postgres::connect(url, NoTls).await?;

        // The connection future drives the socket and must be polled for the
        // client to work at all.
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("postgres connection error: {error}");
            }
        });

        Ok(Self { client })
    }

    pub async fn migrate(&self) -> eyre::Result<()> {
        self.client.batch_execute(include_str!("schema.sql")).await?;
        Ok(())
    }

    /// Drops everything this bot owns and rebuilds it. Destructive by design —
    /// only called behind an explicit flag.
    pub async fn reset(&self) -> eyre::Result<()> {
        self.client
            .batch_execute(
                "DROP VIEW IF EXISTS recent_chat, online_now, session_stats, player_positions_latest, kill_leaderboard, player_kills CASCADE;
                 DROP TABLE IF EXISTS chat_messages, player_events, player_positions, player_names, players, sessions CASCADE;",
            )
            .await?;
        self.migrate().await
    }

    /// Closes sessions a previous run left open, so a hard kill does not leave
    /// rows with a NULL ended_at forever.
    pub async fn close_stale_sessions(&self) -> eyre::Result<u64> {
        let closed = self
            .client
            .execute(
                "UPDATE sessions SET ended_at = COALESCE(
                     (SELECT max(occurred_at) FROM player_events e WHERE e.session_id = sessions.id),
                     started_at
                 ), end_reason = COALESCE(end_reason, 'stale: process exited without a clean shutdown')
                 WHERE ended_at IS NULL",
                &[],
            )
            .await?;
        Ok(closed)
    }

    pub async fn start_session(
        &self,
        host: &str,
        port: i32,
        target_version: &str,
        bot_username: Option<&str>,
    ) -> eyre::Result<i64> {
        let row = self
            .client
            .query_one(
                "INSERT INTO sessions (server_host, server_port, target_version, client, bot_username)
                 VALUES ($1, $2, $3, 'azalea', $4) RETURNING id",
                &[&host, &port, &target_version, &bot_username],
            )
            .await?;
        Ok(row.get(0))
    }

    pub async fn end_session(&self, id: i64, reason: &str) -> eyre::Result<()> {
        self.client
            .execute(
                "UPDATE sessions SET ended_at = now(), end_reason = $2 WHERE id = $1 AND ended_at IS NULL",
                &[&id, &reason],
            )
            .await?;
        Ok(())
    }

    pub async fn set_bot_username(&self, id: i64, username: &str) -> eyre::Result<()> {
        self.client
            .execute("UPDATE sessions SET bot_username = $2 WHERE id = $1", &[&id, &username])
            .await?;
        Ok(())
    }

    /// Spawns the background writer and hands back a cheap clonable handle.
    ///
    /// Takes `Arc<Self>` rather than `self` so the caller keeps the handle it
    /// needs for session lifecycle calls (`end_session`, `set_bot_username`).
    pub fn spawn_writer(self: std::sync::Arc<Self>) -> Writer {
        let (tx, mut rx) = mpsc::unbounded_channel::<Write>();

        tokio::spawn(async move {
            let mut pending: Vec<Write> = Vec::new();
            let mut ticker = tokio::time::interval(FLUSH_INTERVAL);

            loop {
                tokio::select! {
                    received = rx.recv() => match received {
                        Some(write) => {
                            pending.push(write);
                            if pending.len() >= FLUSH_MAX_ROWS {
                                self.flush(&mut pending).await;
                            }
                        }
                        // Channel closed: drain what is left and stop.
                        None => {
                            self.flush(&mut pending).await;
                            break;
                        }
                    },
                    _ = ticker.tick() => self.flush(&mut pending).await,
                }
            }
        });

        Writer { tx }
    }

    async fn flush(&self, pending: &mut Vec<Write>) {
        if pending.is_empty() {
            return;
        }

        for write in pending.drain(..) {
            let result = match write {
                Write::Chat(row) => self.insert_chat(&row).await,
                Write::Event(row) => self.insert_event(&row).await,
            };
            if let Err(error) = result {
                eprintln!("db write failed: {error}");
            }
        }
    }

    async fn insert_chat(&self, row: &ChatRow) -> eyre::Result<()> {
        self.client
            .execute(
                "INSERT INTO chat_messages
                   (session_id, received_at, source, kind, sender_name, sender_uuid,
                    subject_name, killer_name, content, plain_text, ansi, translate_key, raw_json)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
                &[
                    &row.session_id,
                    &row.received_at,
                    &row.source,
                    &row.kind,
                    &row.sender_name,
                    &row.sender_uuid,
                    &row.subject_name,
                    &row.killer_name,
                    &row.content,
                    &row.plain_text,
                    &row.ansi,
                    &row.translate_key,
                    &row.raw_json,
                ],
            )
            .await?;

        if let (Some(uuid), Some(name)) = (row.sender_uuid, row.sender_name.as_ref()) {
            self.upsert_player(uuid, name).await?;
        }
        Ok(())
    }

    async fn insert_event(&self, row: &PlayerEventRow) -> eyre::Result<()> {
        self.client
            .execute(
                "INSERT INTO player_events
                   (session_id, server_host, occurred_at, event_type, source, player_name, player_uuid, raw_text)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                &[
                    &row.session_id,
                    &row.server_host,
                    &row.occurred_at,
                    &row.event_type,
                    &row.source,
                    &row.player_name,
                    &row.player_uuid,
                    &row.raw_text,
                ],
            )
            .await?;

        if let Some(uuid) = row.player_uuid {
            self.upsert_player(uuid, &row.player_name).await?;
        }
        Ok(())
    }

    async fn upsert_player(&self, uuid: Uuid, name: &str) -> eyre::Result<()> {
        self.client
            .execute(
                "INSERT INTO players (uuid, name) VALUES ($1, $2)
                 ON CONFLICT (uuid) DO UPDATE SET name = EXCLUDED.name, last_seen = now()",
                &[&uuid, &name],
            )
            .await?;

        // Keeps the history a plain overwrite would destroy.
        self.client
            .execute(
                "INSERT INTO player_names (uuid, name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                &[&uuid, &name],
            )
            .await?;
        Ok(())
    }

    pub async fn save_plugin_scan(
        &self,
        session_id: i64,
        server_host: &str,
        server_brand: Option<&str>,
        methods: &[String],
        plugins: &serde_json::Value,
        raw_notes: Option<&str>,
    ) -> eyre::Result<()> {
        self.client
            .execute(
                "INSERT INTO server_plugin_scans
                   (session_id, server_host, server_brand, methods, plugins, raw_notes)
                 VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &session_id,
                    &server_host,
                    &server_brand,
                    &methods,
                    &plugins,
                    &raw_notes,
                ],
            )
            .await?;
        Ok(())
    }
}
