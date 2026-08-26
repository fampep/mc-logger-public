//! Postgres writer.
//!
//! Writes go through an unbounded channel to a background task that batches
//! them, so a busy server's chat never blocks the bot's event loop. The
//! mineflayer version did the same thing for the same reason: DonutSMP pushed
//! over 150 messages in 20 seconds.

use std::time::Duration;

use chrono::{DateTime, Utc};
use mc_stream::{ChatEvent, PlayerEvent, StreamEvent, StreamPublisher};
use tokio::sync::mpsc;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

const FLUSH_INTERVAL: Duration = Duration::from_millis(1000);
const FLUSH_MAX_ROWS: usize = 1000;
/// How often the daily rollups are rebuilt. They replaced the per-row
/// counter triggers, so this is the only thing keeping `logger_stats` warm.
const ROLLUP_INTERVAL: Duration = Duration::from_secs(60);

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
    /// Decorated speaker (`[MVP] DesBob`, `<[M] SawgerGG>`) for Discord display.
    pub sender_label: Option<String>,
    pub plain_text: String,
    /// The line as Minecraft drew it, ANSI and all. Not stored — it rides the
    /// live stream so consoles can show the feed in the server's own colours.
    pub ansi: Option<String>,
}

#[derive(Debug)]
pub struct PlayerEventRow {
    pub session_id: i64,
    pub occurred_at: DateTime<Utc>,
    pub event_type: String,
    pub source: String,
    pub player_name: String,
    pub player_uuid: Option<Uuid>,
    /// Set when a chat line announced this; tab-list presence has none.
    pub ansi: Option<String>,
}

#[derive(Debug)]
pub enum Write {
    Chat(Box<ChatRow>),
    Event(Box<PlayerEventRow>),
    /// A restart countdown ran out. The server is about to drop everyone and
    /// will not send a tab-list removal for each player first, so presence has
    /// to be retired in one go or the online list stays full of ghosts until
    /// the bot reconnects.
    ServerRestart {
        session_id: i64,
        at: DateTime<Utc>,
    },
    /// The tab list as it looked once the post-login flood had settled.
    ///
    /// Anyone on it whose last logged event was a leave came back while the
    /// bot was not watching. That is overwhelmingly a server restart: the
    /// countdown retires the whole list in one go, the bot is dropped with
    /// everyone else, and by the time it is back the players have rejoined
    /// unseen. Without this they sit in the log as a leave with no matching
    /// join, so they never come back on for stats, leaderboards, or playtime.
    ReconcileOnline {
        session_id: i64,
        at: DateTime<Utc>,
        players: Vec<String>,
    },
}

#[derive(Clone)]
pub struct Writer {
    /// While false every row is dropped — both the database write and the
    /// stream publish. A standby logger is in-game the whole time; it just must
    /// not duplicate the primary's rows while the primary is healthy.
    active: std::sync::Arc<std::sync::atomic::AtomicBool>,
    tx: mpsc::UnboundedSender<Write>,
    stream: Option<StreamPublisher>,
    server_host: String,
}

impl Writer {
    /// Handle for the standby controller to switch logging on and off.
    pub fn gate(&self) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
        std::sync::Arc::clone(&self.active)
    }

    pub fn logging(&self) -> bool {
        self.active.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn chat(&self, row: ChatRow) {
        if !self.logging() {
            return;
        }
        if let Some(stream) = &self.stream {
            let packed = row.kind.chars().next().unwrap_or('?');
            if packed != 'j' && packed != 'l' {
                stream.publish(StreamEvent::Chat(ChatEvent {
                    seq: None,
                    id: None,
                    ts: row.received_at,
                    kind: row.kind.clone(),
                    sender_name: row.sender_name.clone(),
                    sender_label: row.sender_label.clone(),
                    subject_name: row.subject_name.clone(),
                    killer_name: row.killer_name.clone(),
                    content: None,
                    plain_text: row.plain_text.clone(),
                    ansi: row.ansi.clone(),
                    server_host: Some(self.server_host.clone()),
                }));
            }
        }
        let _ = self.tx.send(Write::Chat(Box::new(row)));
    }

    pub fn event(&self, row: PlayerEventRow) {
        if !self.logging() {
            return;
        }
        if let Some(stream) = &self.stream {
            let packed = row.event_type.chars().next().unwrap_or('?');
            if packed != 's' && packed != 'p' {
                stream.publish(StreamEvent::PlayerEvent(PlayerEvent {
                    seq: None,
                    ts: row.occurred_at,
                    event_type: row.event_type.clone(),
                    source: row.source.clone(),
                    player_name: row.player_name.clone(),
                    ansi: row.ansi.clone(),
                    server_host: Some(self.server_host.clone()),
                }));
            }
        }
        let _ = self.tx.send(Write::Event(Box::new(row)));
    }

    /// Everyone on the tab list is about to be disconnected by a restart.
    pub fn server_restart(&self, session_id: i64, at: DateTime<Utc>) {
        if !self.logging() {
            return;
        }
        let _ = self.tx.send(Write::ServerRestart { session_id, at });
    }

    /// Hand over the settled tab list so joins missed while the bot was away
    /// can be filled in. Goes down the same channel as the events it has to be
    /// judged against, which is what keeps it behind them in the batch.
    pub fn reconcile_online(&self, session_id: i64, at: DateTime<Utc>, players: Vec<String>) {
        if !self.logging() || players.is_empty() {
            return;
        }
        let _ = self.tx.send(Write::ReconcileOnline {
            session_id,
            at,
            players,
        });
    }
}

pub struct Db {
    client: Client,
    /// `sessions.client` for rows this process opens. A standby's sessions have
    /// to be distinguishable from the primary's, or two loggers on one server
    /// read as a single connection flapping.
    session_client: &'static str,
}

impl Db {
    pub async fn connect(url: &str) -> eyre::Result<Self> {
        let (client, connection) = tokio_postgres::connect(url, NoTls).await?;

        // The connection future drives the socket and must be polled for the
        // client to work at all.
        //
        // When it ends, the client is dead for good — tokio_postgres does not
        // reconnect, and every later query returns "connection closed". A
        // logger that stays in game in that state looks healthy while writing
        // nothing: one instance failed 1,778 writes in a row before anyone
        // noticed. Exiting hands the problem to the supervisor, which restarts
        // us with a live connection, exactly as the offline-too-long path does.
        tokio::spawn(async move {
            match connection.await {
                Ok(()) => eprintln!("postgres connection closed"),
                Err(error) => eprintln!("postgres connection error: {error}"),
            }
            eprintln!("exiting so the supervisor restarts us with a working database");
            std::process::exit(1);
        });

        let standby = crate::standby::Role::from_env(
            &std::env::var("LOGGER_ROLE").unwrap_or_default(),
        )
        .is_standby();
        Ok(Self {
            client,
            session_client: if standby { "azalea-standby" } else { "azalea" },
        })
    }

    pub async fn migrate(&self) -> eyre::Result<()> {
        self.client.batch_execute(include_str!("schema.sql")).await?;
        self.client.batch_execute("SELECT ensure_month_partitions();").await?;
        Ok(())
    }

    /// Drops everything this bot owns and rebuilds it. Destructive by design —
    /// only called behind an explicit flag.
    pub async fn reset(&self) -> eyre::Result<()> {
        self.client
            .batch_execute(
                "DROP VIEW IF EXISTS recent_chat, online_now, session_stats, player_positions_latest,
                                     kill_leaderboard, player_kills, logger_stats,
                                     chat_messages, player_events CASCADE;
                 DROP TABLE IF EXISTS chat_messages_raw, player_events_raw, chat_messages, player_events,
                                      player_presence, player_daily, stats_daily, name_dict,
                                      player_positions, player_names, players, sessions CASCADE;",
            )
            .await?;
        self.migrate().await
    }

    /// Closes sessions a previous run left open, so a hard kill does not leave
    /// rows with a NULL ended_at forever.
    ///
    /// Skips sessions that are still writing — multiple loggers can share one
    /// database (6b6t lobby + survival). Closing those would zero the other
    /// bot's online count until it restarted.
    pub async fn close_stale_sessions(&self) -> eyre::Result<u64> {
        let closed = self
            .client
            .execute(
                "UPDATE sessions SET ended_at = COALESCE(
                     -- Sourced from player_presence, not player_events: r8 drops
                     -- the per-session index on the log, so an unbounded
                     -- max(occurred_at) per session would scan the whole table.
                     (SELECT max(pp.at) FROM player_presence pp
                       WHERE pp.session_id = sessions.id::int),
                     started_at
                 ), end_reason = COALESCE(end_reason, 'stale: process exited without a clean shutdown')
                 WHERE ended_at IS NULL
                   AND NOT EXISTS (
                     SELECT 1 FROM player_events e
                     WHERE e.session_id = sessions.id
                       AND e.occurred_at > now() - interval '3 minutes'
                   )
                   AND NOT EXISTS (
                     SELECT 1 FROM chat_messages c
                     WHERE c.session_id = sessions.id
                       AND c.received_at > now() - interval '3 minutes'
                   )
                   -- Never close a session a logger is still heartbeating on.
                   -- With a standby sharing the database, or any second
                   -- instance restarting, silence is not evidence of death: a
                   -- quiet server produces no rows for hours, and closing the
                   -- live session makes online_now report an empty server.
                   AND NOT EXISTS (
                     SELECT 1 FROM logger_heartbeats h
                     WHERE h.session_id = sessions.id
                       AND h.connected
                       AND h.updated_at > now() - interval '2 minutes'
                   )",
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
        let client_label = self.session_client;
        let row = self
            .client
            .query_one(
                "INSERT INTO sessions (server_host, server_port, target_version, client, bot_username)
                 VALUES ($1, $2, $3, $5, $4) RETURNING id",
                &[&host, &port, &target_version, &bot_username, &client_label],
            )
            .await?;
        Ok(row.get(0))
    }

    /// Upsert this logger's heartbeat row.
    #[allow(clippy::too_many_arguments)]
    pub async fn heartbeat(
        &self,
        instance: &str,
        role: &str,
        host: &str,
        session_id: i64,
        bot_username: Option<&str>,
        connected: bool,
        writing: bool,
    ) -> eyre::Result<()> {
        let current = self
            .client
            .query_opt(
                "SELECT role, server_host, session_id, bot_username, connected, writing, reported_online
                   FROM logger_heartbeats
                  WHERE instance = $1",
                &[&instance],
            )
            .await?;

        self.client
            .execute(
                "INSERT INTO logger_heartbeats
                   (instance, role, server_host, session_id, bot_username, connected, writing, updated_at)
                 VALUES ($1,$2,$3,$4,$5,$6,$7, now())
                 ON CONFLICT (instance) DO UPDATE SET
                   role = EXCLUDED.role,
                   server_host = EXCLUDED.server_host,
                   session_id = EXCLUDED.session_id,
                   bot_username = EXCLUDED.bot_username,
                   connected = EXCLUDED.connected,
                   writing = EXCLUDED.writing,
                   updated_at = now()",
                &[
                    &instance,
                    &role,
                    &host,
                    &session_id,
                    &bot_username,
                    &connected,
                    &writing,
                ],
            )
            .await?;

        let changed = match current {
            None => true,
            Some(row) => {
                let prev_role: String = row.get(0);
                let prev_host: String = row.get(1);
                let prev_session_id: Option<i64> = row.get(2);
                let prev_bot_username: Option<String> = row.get(3);
                let prev_connected: bool = row.get(4);
                let prev_writing: bool = row.get(5);
                prev_role != role
                    || prev_host != host
                    || prev_session_id != Some(session_id)
                    || prev_bot_username.as_deref() != bot_username
                    || prev_connected != connected
                    || prev_writing != writing
            }
        };

        if changed {
            let reported_online: Option<i32> = current.as_ref().and_then(|row| row.get(6));
            self.client
                .execute(
                    "INSERT INTO logger_heartbeat_samples
                       (instance, role, server_host, session_id, bot_username, connected, writing, reported_online)
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                    &[
                        &instance,
                        &role,
                        &host,
                        &session_id,
                        &bot_username,
                        &connected,
                        &writing,
                        &reported_online,
                    ],
                )
                .await?;
        }
        Ok(())
    }

    /// The server's own tab-header player count, parsed and written straight
    /// through — no batching, no waiting on the roster this same connection
    /// is separately reconstructing player-by-player.
    pub async fn set_reported_online(&self, instance: &str, count: i32) -> eyre::Result<()> {
        let row = self
            .client
            .query_opt(
                "UPDATE logger_heartbeats
                    SET reported_online = $2, reported_online_at = now()
                  WHERE instance = $1
                RETURNING role, server_host, session_id, bot_username, connected, writing, reported_online",
                &[&instance, &count],
            )
            .await?;

        if let Some(row) = row {
            let role: String = row.get(0);
            let host: String = row.get(1);
            let session_id: Option<i64> = row.get(2);
            let bot_username: Option<String> = row.get(3);
            let connected: bool = row.get(4);
            let writing: bool = row.get(5);
            let reported_online: Option<i32> = row.get(6);
            self.client
                .execute(
                    "INSERT INTO logger_heartbeat_samples
                       (instance, role, server_host, session_id, bot_username, connected, writing, reported_online)
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                    &[
                        &instance,
                        &role,
                        &host,
                        &session_id,
                        &bot_username,
                        &connected,
                        &writing,
                        &reported_online,
                    ],
                )
                .await?;
        }
        Ok(())
    }

    /// Is another logger for `host` connected, logging, and heartbeating within
    /// `stale_secs`? A standby only stays quiet while this is true.
    pub async fn primary_is_live(
        &self,
        host: &str,
        exclude_instance: &str,
        stale_secs: i64,
    ) -> eyre::Result<Option<String>> {
        let row = self
            .client
            .query_opt(
                "SELECT instance FROM logger_heartbeats
                  WHERE server_host = $1
                    AND instance <> $2
                    AND role = 'primary'
                    AND connected
                    AND updated_at > now() - make_interval(secs => $3::double precision)
                  ORDER BY updated_at DESC
                  LIMIT 1",
                &[&host, &exclude_instance, &(stale_secs as f64)],
            )
            .await?;
        Ok(row.map(|r| r.get(0)))
    }

    /// Mark this logger as gone; called on a clean shutdown so a standby can
    /// take over immediately instead of waiting out the staleness window.
    pub async fn clear_heartbeat(&self, instance: &str) -> eyre::Result<()> {
        self.client
            .execute(
                "UPDATE logger_heartbeats SET connected = false, writing = false, updated_at = now()
                  WHERE instance = $1",
                &[&instance],
            )
            .await?;
        Ok(())
    }

    pub async fn end_session(&self, id: i64, reason: &str) -> eyre::Result<()> {
        self.client
            .execute(
                "UPDATE sessions SET ended_at = now(), end_reason = $2 WHERE id = $1 AND ended_at IS NULL",
                &[&id, &reason],
            )
            .await?;
        // Snapshots only matter while the session is live.
        self.client
            .execute(
                r#"DELETE FROM player_events_raw WHERE session_id = $1 AND event_type = 's'::"char""#,
                &[&Self::narrow_session(id)],
            )
            .await?;
        // Presence is per live session, so a closed session's rows are dead
        // weight. Dropping them keeps player_presence bounded by who is
        // actually online rather than by how many sessions have ever run.
        self.client
            .execute(
                "DELETE FROM player_presence WHERE session_id = $1",
                &[&Self::narrow_session(id)],
            )
            .await?;
        Ok(())
    }

    /// `sessions.id` is a bigint but the log tables reference it as int4 —
    /// sessions are counted in the thousands, so the narrow column always fits.
    fn narrow_session(id: i64) -> i32 {
        i32::try_from(id).unwrap_or(i32::MAX)
    }

    pub async fn set_bot_username(&self, id: i64, username: &str) -> eyre::Result<()> {
        self.client
            .execute("UPDATE sessions SET bot_username = $2 WHERE id = $1", &[&id, &username])
            .await?;
        // One live session per bot username — a reconnect must not leave the
        // previous row open beside the lobby (or any other) logger.
        self.client
            .execute(
                "UPDATE sessions SET ended_at = now(),
                        end_reason = COALESCE(end_reason, 'superseded by newer session')
                 WHERE ended_at IS NULL
                   AND id <> $1
                   AND bot_username IS NOT NULL
                   AND lower(bot_username) = lower($2)",
                &[&id, &username],
            )
            .await?;
        self.client
            .execute(
                r#"DELETE FROM player_events_raw pe
                 USING sessions s
                 WHERE pe.session_id = s.id::int
                   AND pe.event_type = 's'::"char"
                   AND s.ended_at IS NOT NULL
                   AND s.bot_username IS NOT NULL
                   AND lower(s.bot_username) = lower($1)"#,
                &[&username],
            )
            .await?;
        self.client
            .execute(
                "DELETE FROM player_presence pp
                 USING sessions s
                 WHERE pp.session_id = s.id::int
                   AND s.ended_at IS NOT NULL
                   AND s.bot_username IS NOT NULL
                   AND lower(s.bot_username) = lower($1)",
                &[&username],
            )
            .await?;
        Ok(())
    }

    /// Upsert the live queue probe row and append a sample when the state changes.
    pub async fn upsert_queue_status(
        &self,
        position: Option<i32>,
        raw_text: &str,
        in_queue: bool,
    ) -> eyre::Result<()> {
        let prev = self
            .client
            .query_opt("SELECT position, in_queue FROM queue_status WHERE id = true", &[])
            .await?;
        let prev_position: Option<i32> = prev.as_ref().and_then(|r| r.get(0));
        let prev_in_queue: Option<bool> = prev.as_ref().map(|r| r.get(1));

        self.client
            .execute(
                r#"INSERT INTO queue_status (id, position, raw_text, in_queue, updated_at)
                   VALUES (true, $1, $2, $3, now())
                   ON CONFLICT (id) DO UPDATE SET
                     position = EXCLUDED.position,
                     raw_text = EXCLUDED.raw_text,
                     in_queue = EXCLUDED.in_queue,
                     updated_at = now()"#,
                &[&position, &raw_text, &in_queue],
            )
            .await?;

        if prev_position != position || prev_in_queue != Some(in_queue) {
            self.client
                .execute(
                    "INSERT INTO queue_samples (position) VALUES ($1)",
                    &[&position],
                )
                .await?;

            let today = chrono::Utc::now().date_naive();
            self.client
                .execute("SELECT refresh_queue_daily($1, $1)", &[&today])
                .await?;
        }
        Ok(())
    }

    /// Spawns the background writer and hands back a cheap clonable handle.
    ///
    /// Takes `Arc<Self>` rather than `self` so the caller keeps the handle it
    /// needs for session lifecycle calls (`end_session`, `set_bot_username`).
    /// `logging` is the writer's initial gate state: `true` for a normal
    /// logger, `false` for a standby that must wait for the primary to drop.
    pub fn spawn_writer(
        self: std::sync::Arc<Self>,
        server_host: String,
        stream: Option<StreamPublisher>,
        logging: bool,
    ) -> Writer {
        let (tx, mut rx) = mpsc::unbounded_channel::<Write>();
        let host = server_host.clone();

        tokio::spawn(async move {
            let mut flusher = Flusher::new(self, host);
            let mut pending: Vec<Write> = Vec::new();
            let mut ticker = tokio::time::interval(FLUSH_INTERVAL);
            let mut rollup = tokio::time::interval(ROLLUP_INTERVAL);
            // A slow rollup must not queue up ticks and then fire them back to
            // back the moment it finishes.
            rollup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    received = rx.recv() => match received {
                        Some(write) => {
                            pending.push(write);
                            if pending.len() >= FLUSH_MAX_ROWS {
                                flusher.flush(&mut pending).await;
                            }
                        }
                        // Channel closed: drain what is left and stop.
                        None => {
                            flusher.flush(&mut pending).await;
                            break;
                        }
                    },
                    _ = ticker.tick() => flusher.flush(&mut pending).await,
                    _ = rollup.tick() => flusher.refresh_rollups().await,
                }
            }
        });

        Writer {
            active: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(logging)),
            tx,
            stream,
            server_host,
        }
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

/// Rank tags / cosmetic symbols in front of the IGN (`[Prime]`, `[M]`, `❄`).
/// Batched write side.
///
/// The old path cost four round trips per chat line: one INSERT, then up to
/// three `touch_player` statements. At a few hundred lines a second that is
/// what actually limited the logger, not Postgres.
///
/// This owns its caches outright. It lives in the single writer task, so the
/// maps need no locking: only one flush runs at a time.
struct Flusher {
    db: std::sync::Arc<Db>,
    server_host: String,
    /// lower(name) -> name_dict.id. The point of r8: a name reaches disk once
    /// and is an int on every row after that.
    names: std::collections::HashMap<String, i32>,
    /// lower(name) -> whether the uuid on file for it came from the server
    /// (true) rather than being computed from the name (false, used when a
    /// name is only ever mentioned in chat — a death or kill line naming a
    /// bystander, say — before any sighting that carries a real uuid). A
    /// synthetic uuid is corrected the moment a real one shows up instead of
    /// being skipped forever by the "already touched" fast path below —
    /// premium accounts first mentioned that way would otherwise keep a fake
    /// uuid, and the wrong skin, for the rest of the process's life.
    known_players: std::collections::HashMap<String, bool>,
    /// lower(name) -> rank last written, so an unchanged rank costs nothing.
    ranks: std::collections::HashMap<String, String>,
    /// UTC day the rollups last covered, so the previous day is closed out
    /// exactly once after midnight.
    rollup_day: Option<chrono::NaiveDate>,
}

/// Dictionary keys are not player names: a death line can name a mob
/// ("Wither Skeleton"), which `normalize_mc_name` correctly rejects for the
/// `players` table but which still has to survive in the log.
fn dict_key(name: &str) -> Option<&str> {
    let name = name.trim();
    if name.is_empty() || name.len() > 64 {
        return None;
    }
    Some(name)
}

fn code(value: &str) -> String {
    value.chars().next().unwrap_or('u').to_string()
}

impl Flusher {
    fn new(db: std::sync::Arc<Db>, server_host: String) -> Self {
        Self {
            db,
            server_host,
            names: std::collections::HashMap::new(),
            known_players: std::collections::HashMap::new(),
            ranks: std::collections::HashMap::new(),
            rollup_day: None,
        }
    }

    async fn flush(&mut self, pending: &mut Vec<Write>) {
        if pending.is_empty() {
            return;
        }
        let batch: Vec<Write> = pending.drain(..).collect();
        if let Err(error) = self.write_batch(&batch).await {
            eprintln!("db write failed ({} rows): {error:#}", batch.len());
        }
    }

    async fn write_batch(&mut self, batch: &[Write]) -> eyre::Result<()> {
        // One round trip resolves every name the batch mentions.
        let mut wanted: Vec<&str> = Vec::new();
        for write in batch {
            match write {
                Write::Chat(row) => {
                    for name in [
                        row.sender_name.as_deref(),
                        row.subject_name.as_deref(),
                        row.killer_name.as_deref(),
                    ] {
                        if let Some(name) = name.and_then(dict_key) {
                            wanted.push(name);
                        }
                    }
                }
                Write::Event(row) => {
                    if let Some(name) = dict_key(&row.player_name) {
                        wanted.push(name);
                    }
                }
                Write::ReconcileOnline { players, .. } => {
                    for player in players {
                        if let Some(name) = dict_key(player) {
                            wanted.push(name);
                        }
                    }
                }
                Write::ServerRestart { .. } => {}
            }
        }
        self.resolve_names(&wanted).await?;

        self.write_chat(batch).await?;
        self.write_events(batch).await?;
        self.write_restarts(batch).await?;
        self.write_rejoins(batch).await?;
        self.touch_players(batch).await?;
        Ok(())
    }

    /// Fills `self.names` for everything in `wanted`, inserting whatever the
    /// dictionary is missing. Steady state is zero queries: the cache already
    /// holds every regular on the server.
    async fn resolve_names(&mut self, wanted: &[&str]) -> eyre::Result<()> {
        let mut missing: Vec<String> = Vec::new();
        let mut queued: std::collections::HashSet<String> = std::collections::HashSet::new();
        for name in wanted {
            let key = name.to_lowercase();
            if self.names.contains_key(&key) {
                continue;
            }
            if queued.insert(key) {
                missing.push((*name).to_string());
            }
        }
        if missing.is_empty() {
            return Ok(());
        }

        self.db
            .client
            .execute(
                "INSERT INTO name_dict (name)
                 SELECT DISTINCT ON (lower(n)) n FROM unnest($1::text[]) AS t(n)
                 ORDER BY lower(n)
                 ON CONFLICT (lower(name)) DO NOTHING",
                &[&missing],
            )
            .await?;

        let lowered: Vec<String> = missing.iter().map(|n| n.to_lowercase()).collect();
        let rows = self
            .db
            .client
            .query(
                "SELECT id, lower(name) FROM name_dict WHERE lower(name) = ANY($1::text[])",
                &[&lowered],
            )
            .await?;
        for row in rows {
            let id: i32 = row.get(0);
            let key: String = row.get(1);
            self.names.insert(key, id);
        }
        Ok(())
    }

    fn name_id(&self, name: Option<&str>) -> Option<i32> {
        let name = name.and_then(dict_key)?;
        self.names.get(&name.to_lowercase()).copied()
    }

    /// Sessions are counted in the thousands, so the narrow int4 column always
    /// fits. Anything that somehow does not is written unlinked rather than
    /// dropped.
    fn session_id(value: i64) -> Option<i32> {
        i32::try_from(value).ok()
    }

    async fn write_chat(&mut self, batch: &[Write]) -> eyre::Result<()> {
        let mut session: Vec<Option<i32>> = Vec::new();
        let mut at: Vec<DateTime<Utc>> = Vec::new();
        let mut sender: Vec<Option<i32>> = Vec::new();
        let mut subject: Vec<Option<i32>> = Vec::new();
        let mut killer: Vec<Option<i32>> = Vec::new();
        let mut source: Vec<String> = Vec::new();
        let mut kind: Vec<String> = Vec::new();
        let mut text: Vec<String> = Vec::new();

        for write in batch {
            let Write::Chat(row) = write else { continue };
            session.push(Self::session_id(row.session_id));
            at.push(row.received_at);
            sender.push(self.name_id(row.sender_name.as_deref()));
            subject.push(self.name_id(row.subject_name.as_deref()));
            killer.push(self.name_id(row.killer_name.as_deref()));
            source.push(code(&row.source));
            kind.push(code(&row.kind));
            text.push(row.plain_text.clone());
        }
        if at.is_empty() {
            return Ok(());
        }

        self.db
            .client
            .execute(
                "INSERT INTO chat_messages_raw
                   (session_id, received_at, sender_id, subject_id, killer_id, source, kind, plain_text)
                 SELECT s, r, snd, subj, kil, left(src, 1)::\"char\", left(knd, 1)::\"char\", txt
                 FROM unnest($1::int[], $2::timestamptz[], $3::int[], $4::int[],
                             $5::int[], $6::text[], $7::text[], $8::text[])
                      AS t(s, r, snd, subj, kil, src, knd, txt)",
                &[&session, &at, &sender, &subject, &killer, &source, &kind, &text],
            )
            .await?;
        Ok(())
    }

    async fn write_events(&mut self, batch: &[Write]) -> eyre::Result<()> {
        let mut session: Vec<Option<i32>> = Vec::new();
        let mut at: Vec<DateTime<Utc>> = Vec::new();
        let mut player: Vec<i32> = Vec::new();
        let mut event_type: Vec<String> = Vec::new();
        let mut source: Vec<String> = Vec::new();
        let mut uuid: Vec<Option<Uuid>> = Vec::new();

        // Presence is state, so only the last event per (session, player) in a
        // batch matters. online_now reads this table instead of scanning the
        // whole event log the way the old view did.
        let mut presence: std::collections::HashMap<(i32, i32), (bool, DateTime<Utc>)> =
            std::collections::HashMap::new();

        for write in batch {
            let Write::Event(row) = write else { continue };
            let Some(player_id) = self.name_id(Some(&row.player_name)) else {
                continue;
            };
            let session_id = Self::session_id(row.session_id);
            let et = code(&row.event_type);
            let src = code(&row.source);

            if src == "t" {
                if let Some(session_id) = session_id {
                    // 's' is a tab-list appearance and 'p' a disappearance, so
                    // 'p' means gone exactly as 'l' does. The pre-r8 online_now
                    // view excluded both; leaving 'p' out here kept players
                    // listed as online after they had dropped off the tab list.
                    let present = et != "l" && et != "p";
                    presence
                        .entry((session_id, player_id))
                        .and_modify(|slot| {
                            if row.occurred_at >= slot.1 {
                                *slot = (present, row.occurred_at);
                            }
                        })
                        .or_insert((present, row.occurred_at));
                }
            }

            // Tab-list presence markers are state, not history, and nothing
            // reads them back: the stream skips them, online_now reads
            // player_presence, and the rollups count only 'j'/'l'. They were
            // 37% of the event table on 6b6t. Recorded as presence above, then
            // dropped rather than written to the log.
            if et == "p" || et == "s" {
                continue;
            }

            session.push(session_id);
            at.push(row.occurred_at);
            player.push(player_id);
            event_type.push(et);
            source.push(src);
            // The row-level uuid only exists where it disagrees with the
            // canonical players.uuid; the common case is resolved through the
            // view, so nothing is stored here.
            uuid.push(None);
        }
        if at.is_empty() {
            return Ok(());
        }

        self.db
            .client
            .execute(
                "INSERT INTO player_events_raw
                   (session_id, occurred_at, player_id, event_type, source, player_uuid)
                 SELECT s, o, p, left(et, 1)::\"char\", left(src, 1)::\"char\", u
                 FROM unnest($1::int[], $2::timestamptz[], $3::int[],
                             $4::text[], $5::text[], $6::uuid[])
                      AS t(s, o, p, et, src, u)",
                &[&session, &at, &player, &event_type, &source, &uuid],
            )
            .await?;

        if !presence.is_empty() {
            let mut p_session: Vec<i32> = Vec::new();
            let mut p_player: Vec<i32> = Vec::new();
            let mut p_present: Vec<bool> = Vec::new();
            let mut p_at: Vec<DateTime<Utc>> = Vec::new();
            for ((session_id, player_id), (present, at)) in presence {
                p_session.push(session_id);
                p_player.push(player_id);
                p_present.push(present);
                p_at.push(at);
            }
            self.upsert_presence(&p_session, &p_player, &p_present, &p_at)
                .await?;
        }
        Ok(())
    }

    /// Shared by `write_events` and `write_rejoins`: marks each
    /// `(session_id, player_id)` present/absent as of `at`, keeping only the
    /// latest write per pair (`player_presence.at <= EXCLUDED.at` guards
    /// against an out-of-order batch regressing a newer state).
    async fn upsert_presence(
        &mut self,
        session: &[i32],
        player: &[i32],
        present: &[bool],
        at: &[DateTime<Utc>],
    ) -> eyre::Result<()> {
        self.db
            .client
            .execute(
                "INSERT INTO player_presence (session_id, player_id, server_host, present, at)
                 SELECT s, p, $5::text, pres, ts
                 FROM unnest($1::int[], $2::int[], $3::bool[], $4::timestamptz[])
                      AS t(s, p, pres, ts)
                 ON CONFLICT (session_id, player_id) DO UPDATE
                   SET present = EXCLUDED.present, at = EXCLUDED.at
                   WHERE player_presence.at <= EXCLUDED.at",
                &[&session, &player, &present, &at, &self.server_host],
            )
            .await?;
        Ok(())
    }

    /// Retires everyone still on the tab list because the server is restarting.
    ///
    /// A leave is logged for each of them so join/leave history stays paired —
    /// without it `/stats` playtime treats an unmatched join as "still online"
    /// and counts the time until now. They are written with source 'r' so the
    /// Discord bridge can tell a synthesised restart leave from a real one and
    /// skip posting several hundred of them at once.
    async fn write_restarts(&mut self, batch: &[Write]) -> eyre::Result<()> {
        for write in batch {
            let Write::ServerRestart { session_id, at } = write else {
                continue;
            };
            let Some(session) = Self::session_id(*session_id) else {
                continue;
            };
            let logged = self
                .db
                .client
                .execute(
                    "INSERT INTO player_events_raw
                       (session_id, occurred_at, player_id, event_type, source)
                     SELECT session_id, $2, player_id, 'l'::\"char\", 'r'::\"char\"
                     FROM player_presence
                     WHERE session_id = $1 AND present",
                    &[&session, at],
                )
                .await?;
            self.db
                .client
                .execute(
                    "UPDATE player_presence SET present = false, at = $2
                     WHERE session_id = $1 AND present",
                    &[&session, at],
                )
                .await?;
            if logged > 0 {
                eprintln!("server restart: retired {logged} player(s) from the online list");
            }
        }
        Ok(())
    }

    /// Gives a join back to everyone who rejoined while the bot was away.
    ///
    /// The tab list on reconnect says who is online; the log says who the bot
    /// last saw arrive or leave. Where those disagree -- on the list, but last
    /// seen leaving -- a join happened unwitnessed and is written here.
    ///
    /// Deciding per player, rather than "everything in the first tab flood is a
    /// join", is what keeps this from double counting. A plain reconnect where
    /// nobody actually left leaves every one of those players sitting on an open
    /// join, they match nothing, and no rows are written at all. It also means
    /// the tab-list joins this same batch already wrote are visible by the time
    /// the query runs, so they are not written a second time.
    ///
    /// The rejoins are marked `source = 'r'`, the same marker the restart
    /// leaves carry, because they are the other half of the same event: the
    /// Discord bridge already skips that source and so does not replay several
    /// hundred joins into a channel at once.
    async fn write_rejoins(&mut self, batch: &[Write]) -> eyre::Result<()> {
        for write in batch {
            let Write::ReconcileOnline {
                session_id,
                at,
                players,
            } = write
            else {
                continue;
            };
            let Some(session) = Self::session_id(*session_id) else {
                continue;
            };

            // Names the dictionary knows; resolve_names ran for this batch, so a
            // miss here is a name that could not be stored at all.
            let ids: Vec<i32> = players
                .iter()
                .filter_map(|name| dict_key(name))
                .filter_map(|name| self.names.get(&name.to_lowercase()).copied())
                .collect();
            if ids.is_empty() {
                continue;
            }

            // Presence otherwise only advances one player at a time, as each
            // individual tab packet is handled — on a server that trickles its
            // tab list out slowly (6b6t observed at roughly one entry every
            // 15-30s), that took hours to catch up to a few hundred players,
            // during which online_now drifted well past the real count because
            // departures kept being recorded while arrivals were still
            // trickling in. This snapshot is already settled and complete, so
            // mark everyone in it present in one shot instead of waiting for
            // the trickle to arrive at the same answer.
            let session_col: Vec<i32> = vec![session; ids.len()];
            let present_col: Vec<bool> = vec![true; ids.len()];
            let at_col: Vec<DateTime<Utc>> = vec![*at; ids.len()];
            self.upsert_presence(&session_col, &ids, &present_col, &at_col)
                .await?;

            // `events_player_idx` covers (player_id, occurred_at DESC) for exactly
            // the j/l rows this reads, so it is one index probe per player rather
            // than a scan. Only the hot table is consulted: a player whose last
            // event has already been archived has not been seen in days and is
            // arriving now whatever that row says.
            let logged = self
                .db
                .client
                .execute(
                    r#"WITH candidate AS (
                         SELECT DISTINCT unnest($3::int[]) AS player_id
                       ),
                       last_seen AS (
                         SELECT DISTINCT ON (pe.player_id)
                                pe.player_id, pe.event_type
                           FROM player_events_raw pe
                           JOIN candidate c ON c.player_id = pe.player_id
                          WHERE pe.event_type IN ('j'::"char", 'l'::"char")
                          ORDER BY pe.player_id, pe.occurred_at DESC
                       )
                       INSERT INTO player_events_raw
                         (session_id, occurred_at, player_id, event_type, source)
                       SELECT $1, $2, c.player_id, 'j'::"char", 'r'::"char"
                         FROM candidate c
                         LEFT JOIN last_seen l ON l.player_id = c.player_id
                        WHERE l.event_type IS DISTINCT FROM 'j'::"char""#,
                    &[&session, at, &ids],
                )
                .await?;
            if logged > 0 {
                eprintln!(
                    "reconnect: gave {logged} of {} online player(s) the join we missed",
                    ids.len()
                );
            }

            // The mirror case: `player_presence` already says who this session
            // still thinks is here, cheaply — one row per player, indexed on
            // (session_id, player_id) — versus re-deriving it from the full
            // event log the way the join backfill above does. Anyone marked
            // present who is absent from this settled snapshot left without a
            // leave ever being recorded (a chat line missed during a
            // disconnect, or a departure this trickle hadn't caught up to
            // yet), so both stats and presence retire them here in one shot.
            let missed_leaves = self
                .db
                .client
                .execute(
                    r#"WITH stale AS (
                         SELECT player_id FROM player_presence
                          WHERE session_id = $1 AND present AND player_id != ALL($3::int[])
                       )
                       INSERT INTO player_events_raw
                         (session_id, occurred_at, player_id, event_type, source)
                       SELECT $1, $2, player_id, 'l'::"char", 'r'::"char" FROM stale"#,
                    &[&session, at, &ids],
                )
                .await?;
            self.db
                .client
                .execute(
                    "UPDATE player_presence SET present = false, at = $2
                     WHERE session_id = $1 AND present AND player_id != ALL($3::int[])",
                    &[&session, at, &ids],
                )
                .await?;
            if missed_leaves > 0 {
                eprintln!(
                    "reconnect: retired {missed_leaves} player(s) who left without a leave being recorded"
                );
            }
        }
        Ok(())
    }
    /// Keeps `players` current. One UPDATE bumps last_seen for everyone the
    /// batch mentioned; only genuinely new names cost anything more.
    async fn touch_players(&mut self, batch: &[Write]) -> eyre::Result<()> {
        // lower(name) -> (canonical name, real uuid if the server gave one)
        let mut seen: std::collections::HashMap<String, (String, Option<Uuid>)> =
            std::collections::HashMap::new();
        let mut note = |name: Option<&str>, uuid: Option<Uuid>| {
            let Some(name) = name.and_then(normalize_mc_name) else {
                return;
            };
            let entry = seen
                .entry(name.to_lowercase())
                .or_insert_with(|| (name.to_string(), None));
            if uuid.is_some() {
                entry.1 = uuid;
            }
        };

        for write in batch {
            match write {
                Write::Chat(row) => {
                    note(row.sender_name.as_deref(), row.sender_uuid);
                    note(row.subject_name.as_deref(), None);
                    note(row.killer_name.as_deref(), None);
                }
                Write::Event(row) => note(Some(&row.player_name), row.player_uuid),
                Write::ReconcileOnline { players, .. } => {
                    for player in players {
                        note(Some(player), None);
                    }
                }
                Write::ServerRestart { .. } => {}
            }
        }
        if seen.is_empty() {
            return Ok(());
        }

        let all: Vec<String> = seen.keys().cloned().collect();
        self.db
            .client
            .execute(
                "UPDATE players SET last_seen = now() WHERE lower(name) = ANY($1::text[])",
                &[&all],
            )
            .await?;

        let fresh: Vec<(String, String, Uuid, bool)> = seen
            .iter()
            .filter(|(key, (_, uuid))| match self.known_players.get(*key) {
                None => true,
                Some(true) => false,
                // Had only a synthetic uuid on file — worth another upsert
                // only if this sighting actually carries a real one.
                Some(false) => uuid.is_some(),
            })
            .map(|(key, (name, uuid))| {
                let is_real = uuid.is_some();
                (
                    key.clone(),
                    name.clone(),
                    uuid.unwrap_or_else(|| name_based_player_uuid(name)),
                    is_real,
                )
            })
            .collect();

        if !fresh.is_empty() {
            let uuids: Vec<Uuid> = fresh.iter().map(|(_, _, u, _)| *u).collect();
            let names: Vec<String> = fresh.iter().map(|(_, n, _, _)| n.clone()).collect();

            // A real Mojang uuid wins over the name-based one an earlier
            // chat-only sighting may have created, so free the unique
            // lower(name) slot before inserting.
            self.db
                .client
                .execute(
                    "DELETE FROM players p
                       USING unnest($1::uuid[], $2::text[]) AS t(u, n)
                      WHERE lower(p.name) = lower(t.n) AND p.uuid <> t.u",
                    &[&uuids, &names],
                )
                .await?;

            self.db
                .client
                .execute(
                    "INSERT INTO players (uuid, name, name_id)
                     SELECT t.u, t.n, nd.id
                       FROM unnest($1::uuid[], $2::text[]) AS t(u, n)
                       JOIN name_dict nd ON lower(nd.name) = lower(t.n)
                     ON CONFLICT (uuid) DO UPDATE
                       SET name = EXCLUDED.name,
                           name_id = EXCLUDED.name_id,
                           last_seen = now()",
                    &[&uuids, &names],
                )
                .await?;

            // Keeps the rename history a plain overwrite would destroy.
            self.db
                .client
                .execute(
                    "INSERT INTO player_names (uuid, name)
                     SELECT u, n FROM unnest($1::uuid[], $2::text[]) AS t(u, n)
                     ON CONFLICT DO NOTHING",
                    &[&uuids, &names],
                )
                .await?;

            for (key, _, _, is_real) in fresh {
                self.known_players.insert(key, is_real);
            }
        }

        self.write_ranks(batch).await
    }

    async fn write_ranks(&mut self, batch: &[Write]) -> eyre::Result<()> {
        let mut changed: std::collections::HashMap<String, (String, String)> =
            std::collections::HashMap::new();
        for write in batch {
            let Write::Chat(row) = write else { continue };
            let (Some(name), Some(label)) = (row.sender_name.as_deref(), row.sender_label.as_deref())
            else {
                continue;
            };
            let Some(name) = normalize_mc_name(name) else {
                continue;
            };
            let Some(rank) = rank_prefix(label, name) else {
                continue;
            };
            let key = name.to_lowercase();
            if self.ranks.get(&key) == Some(&rank) {
                continue;
            }
            changed.insert(key, (name.to_string(), rank));
        }
        if changed.is_empty() {
            return Ok(());
        }

        let names: Vec<String> = changed.values().map(|(n, _)| n.clone()).collect();
        let ranks: Vec<String> = changed.values().map(|(_, r)| r.clone()).collect();
        self.db
            .client
            .execute(
                "UPDATE players p SET chat_rank = t.r
                   FROM unnest($1::text[], $2::text[]) AS t(n, r)
                  WHERE lower(p.name) = lower(t.n) AND p.chat_rank IS DISTINCT FROM t.r",
                &[&names, &ranks],
            )
            .await?;

        for (key, (_, rank)) in changed {
            self.ranks.insert(key, rank);
        }
        Ok(())
    }

    /// Rebuilds today's rollup rows, and yesterday's once, just after midnight.
    ///
    /// This is what replaced the per-row triggers. Those UPDATEd a single
    /// counter row on every insert, which serialised every writer in the
    /// database behind one row lock and left a dead tuple each time.
    async fn refresh_rollups(&mut self) {
        let today = Utc::now().date_naive();
        let close_yesterday = self.rollup_day.is_some_and(|day| day < today);
        let from = if close_yesterday {
            today.pred_opt().unwrap_or(today)
        } else {
            today
        };

        let sql = "SELECT refresh_stats_daily($1, $2), refresh_player_daily($1, $2)";
        if let Err(error) = self.db.client.execute(sql, &[&from, &today]).await {
            eprintln!("rollup refresh failed: {error:#}");
            return;
        }

        // ensure_month_partitions only reaches two months ahead, so a logger
        // that stays up longer than that would eventually insert into a range
        // no partition covers. Once a day is plenty and costs nothing.
        if close_yesterday || self.rollup_day.is_none() {
            if let Err(error) = self
                .db
                .client
                .execute("SELECT ensure_month_partitions()", &[])
                .await
            {
                eprintln!("partition maintenance failed: {error:#}");
            }
        }

        self.rollup_day = Some(today);
    }
}

fn rank_prefix(label: &str, name: &str) -> Option<String> {
    if name.is_empty() {
        return None;
    }
    let idx = label.rfind(name)?;
    let before = label[..idx]
        .trim()
        .trim_start_matches('<')
        .trim_end_matches(['<', '>', '»'])
        .trim();
    if before.is_empty() {
        None
    } else {
        Some(before.to_string())
    }
}

/// Stable UUID for players we only know by name (chat join/leave, etc.).
fn name_based_player_uuid(name: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!("mc-logger-player:{}", name.to_lowercase()).as_bytes(),
    )
}

/// Java / Bedrock-ish names: optional `*` / `.` prefix, then 1–16 [A-Za-z0-9_].
fn normalize_mc_name(name: &str) -> Option<&str> {
    let name = name.trim();
    if name.is_empty() || name.len() > 16 {
        return None;
    }
    let body = name
        .strip_prefix('*')
        .or_else(|| name.strip_prefix('.'))
        .unwrap_or(name);
    if body.is_empty() || body.len() > 16 {
        return None;
    }
    if !body
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return None;
    }
    Some(name)
}

#[cfg(test)]
mod rank_prefix_tests {
    use super::rank_prefix;

    #[test]
    fn extracts_chat_prefixes() {
        assert_eq!(
            rank_prefix("[Prime] TheGroupProject", "TheGroupProject").as_deref(),
            Some("[Prime]")
        );
        assert_eq!(
            rank_prefix("[8] [MVP] DesBob [slime]", "DesBob").as_deref(),
            Some("[8] [MVP]")
        );
        assert_eq!(
            rank_prefix("<[M] SawgerGG>", "SawgerGG").as_deref(),
            Some("[M]")
        );
        assert_eq!(rank_prefix("❄ santiahre", "santiahre").as_deref(), Some("❄"));
        assert_eq!(rank_prefix("jointedx21", "jointedx21"), None);
    }
}
