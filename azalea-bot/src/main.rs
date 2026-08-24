//! Minecraft chat logger: azalea + ViaVersion, writing to Postgres.
//!
//! Replaces a mineflayer implementation that could not reach either target
//! server. mineflayer tops out at protocol 1.21.11, so purityvanilla (26.1.2)
//! rejected it as too old; azalea alone speaks exactly one protocol per build,
//! so Constantiam (max 1.21.10) rejected *it* as too new. ViaProxy underneath
//! makes the server version a runtime setting, so one binary reaches both.

mod classify;
mod db;
mod plugin_probe;
mod standby;

use std::sync::Arc;

use azalea::bot::DefaultBotPlugins;
use azalea::client_chat::ChatPacket;
use azalea::pathfinder::goals::BlockPosGoal;
use azalea::prelude::*;
use azalea::registry::builtin::BlockKind;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use azalea::prelude::*;
use azalea::buf::AzBuf;
use azalea::Identifier;
use azalea::protocol::packets::game::{
    s_command_suggestion::ServerboundCommandSuggestion,
    s_custom_payload::ServerboundCustomPayload,
    ClientboundGamePacket,
};
use azalea_buf::UnsizedByteArray;
use azalea_viaversion::ViaVersionPlugin;
use chrono::Utc;

use crate::classify::Kind;
use crate::db::{ChatRow, Db, PlayerEventRow, Writer};
use crate::plugin_probe::{
    merge_plugins, parse_register_channels, plugin_from_channel, plugins_from_chat,
    plugins_from_command_names, plugins_from_register_channels, plugins_from_tab_suggestions,
    PluginSource,
};

/// Everything the handlers need that cannot be meaningfully defaulted.
///
/// This lives in a `OnceLock` rather than in `State` because azalea constructs
/// the handler state with `Default::default()` — even when `set_state` is used —
/// and neither a database handle nor a channel sender has a sensible default.
struct Services {
    writer: Writer,
    db: Arc<Db>,
    /// The session rows written events belong to.
    ///
    /// Mutable because azalea reconnects inside the same process: a dropped
    /// connection ends the session, and the next login has to open a new one.
    /// Keeping the original id instead attributed hours of logging to a session
    /// marked as ended, which in turn made `online_now` — which only trusts an
    /// open session — report an empty server while the bot was connected.
    session_id: std::sync::atomic::AtomicI64,
    /// Logins seen so far. The first one belongs to the session opened at
    /// startup; every later one is a reconnect and needs a session of its own.
    logins: std::sync::atomic::AtomicU32,
    host: String,
    port: i32,
    target_version: String,
    /// Walk into a nether portal after spawning (6b6t's lobby exit).
    enter_portal: bool,
    /// True while a portal-walk task is running. Cleared when it finishes so a
    /// later dump back to the lobby/backup server can try again.
    portal_walking: std::sync::atomic::AtomicBool,
    /// True once actually past the lobby (or immediately, for a server with no
    /// `enter_portal` walk to begin with). Distinct from `!portal_walking`,
    /// which is also false before the walk has even started — that ambiguity
    /// let the lobby's own tab list get counted as real online presence for
    /// the window between Spawn and the walk finishing.
    in_main_world: std::sync::atomic::AtomicBool,
    anti_afk: Option<(u64, u64)>,
    anti_afk_started: std::sync::atomic::AtomicBool,
    /// True between Login and Disconnect. A reconnect that never succeeds
    /// (Microsoft SessionServer rate limits) leaves the process alive but
    /// offline unless we exit and let systemd restart us.
    connected: std::sync::atomic::AtomicBool,
    login_at_ms: std::sync::atomic::AtomicI64,
    plugin_probe: Mutex<PluginProbeState>,
    /// Sit in the 2b2t (etc.) queue, record position, leave at N and rejoin so
    /// the reported spot tracks the real queue length.
    queue_probe: bool,
    queue_leave_at: i32,
    /// Debounce so we only disconnect once per cycle.
    queue_leaving: std::sync::atomic::AtomicBool,
    /// Main (non-probe) logger is waiting in queue limbo — pause chat + presence
    /// so the queue tablist is not treated as "online" and shop spam stays out
    /// of the Discord chat bridge.
    in_queue_limbo: std::sync::atomic::AtomicBool,
    /// Run `/connectionmsgs on` (6b6t) so join/leave appear in chat for Discord.
    connection_msgs: bool,
    /// When false, chat owns stored join/leave (6b6t `/connectionmsgs`).
    /// Tab list still writes presence snapshots so `online_now` stays correct.
    tablist_joins: bool,
    connection_msgs_sent: std::sync::atomic::AtomicBool,
    /// Commands to run after spawning (`STARTUP_COMMANDS=/showspam on`).
    /// Whatever the server prints back lands in the log like any other chat.
    /// A value containing a space has to be quoted -- dotenv rejects it bare.
    startup_commands: Vec<String>,
    /// Re-armed on every login. A server restart disconnects the bot and the
    /// reconnect is a fresh session, so a toggle set in the last one is gone
    /// and has to be set again.
    startup_commands_sent: std::sync::atomic::AtomicBool,
    /// In-game username after spawn; join/leave for this name are not published.
    bot_username: Mutex<Option<String>>,
    /// Handle to the live client, so the Discord relay can speak without going
    /// through the event loop. Cleared on disconnect.
    bot_handle: Mutex<Option<Client>>,
    /// Discord → game relay settings.
    say_enabled: bool,
    say_allow_commands: bool,
    say_prefix: String,
    /// Stay in 6b6t's lobby: leave again if the server dumps us onto a survival
    /// worker. Opt-in via `LOBBY_ONLY=true` — it used to be implied by
    /// `ENTER_PORTAL=false`, which made every ordinary server eligible, and any
    /// spawn that happened to land between y=50 and y=135 was read as "dumped"
    /// and disconnected on a loop.
    lobby_only: bool,
    /// Debounce so the escape disconnect only fires once per cycle.
    lobby_escape_leaving: std::sync::atomic::AtomicBool,
    /// UUIDs currently counted as online from the tab list (listed players).
    tab_online: Mutex<HashMap<Uuid, String>>,
    /// UUIDs hidden with `listed=false` — ignore a following AddPlayer for them.
    tab_hidden: Mutex<HashSet<Uuid>>,
    tab_reconcile_ticks: std::sync::atomic::AtomicU32,
    /// `tab_online.len()` as of the last reconcile tick, to detect when the
    /// post-login/post-queue tab-list flood has stopped growing.
    settle_last_len: std::sync::atomic::AtomicUsize,
    /// Consecutive reconcile ticks with no growth. Reset to 0 the moment
    /// growth resumes, so a brief lull mid-flood cannot fire this early.
    settle_quiet_ticks: std::sync::atomic::AtomicU32,
    /// Whether the settle reconcile has already fired since the last time
    /// tab presence was cleared (login, disconnect, or entering queue limbo).
    settle_done: std::sync::atomic::AtomicBool,
    /// Last player count parsed from the server's own tab-list header/footer
    /// ("Online players: 504") and written to `logger_heartbeats`. -1 means
    /// none seen yet. Kept only to skip redundant writes when it hasn't
    /// changed — the header repeats on nearly every packet regardless.
    reported_online: std::sync::atomic::AtomicI64,
}

#[derive(Default)]
struct PluginProbeState {
    probe_start_ms: i64,
    ticks_since_start: u32,
    saved: bool,
    server_brand: Option<String>,
    tree_plugins: BTreeSet<String>,
    tab_plugins: BTreeSet<String>,
    chat_plugins: BTreeSet<String>,
    register_plugins: BTreeSet<String>,
    channel_plugins: BTreeSet<String>,
    version_alias: Option<String>,
    pending_tab_id: Option<u32>,
    tab_requested: bool,
    chat_requested: bool,
    waiting_chat: bool,
    methods: BTreeSet<String>,
}

impl PluginProbeState {
    fn reset(&mut self) {
        *self = Self::default();
    }
}

impl Services {
    fn session(&self) -> i64 {
        self.session_id.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// How long after login tab-list additions are treated as the initial replay.
/// Large anarchy tab lists can take well over 10s to finish flooding AddPlayer.
const SNAPSHOT_GRACE_MS: i64 = 60_000;
/// Re-check azalea's tab list every N game ticks (~20/s) for missed removes.
const TAB_RECONCILE_TICKS: u32 = 100;
/// Consecutive reconcile checks (each ~5s apart) with no growth in the tab
/// roster before the settle watch fires — about 40s of genuine quiet.
const SETTLE_QUIET_TICKS: u32 = 8;

static SERVICES: std::sync::OnceLock<Services> = std::sync::OnceLock::new();

fn services() -> &'static Services {
    SERVICES.get().expect("services are initialised before the client starts")
}

#[derive(Clone, Component, Default)]
pub struct State {}

#[tokio::main]
async fn main() -> AppExit {
    // systemd sets ENV_FILE for secondary instances (.env.purity, .env.9b9t, …).
    if let Ok(path) = std::env::var("ENV_FILE") {
        if let Err(error) = dotenvy::from_filename_override(&path) {
            eprintln!("cannot load ENV_FILE={path}: {error}");
            std::process::exit(1);
        }
    } else {
        let _ = dotenvy::dotenv();
    }

    let email = require_env("MC_EMAIL", "the Microsoft account to sign in as");
    let database_url = require_env("DATABASE_URL", "the Postgres connection string");
    // A standby instance connects and stays in-game like any other logger, but
    // writes nothing until the primary stops heartbeating.
    let role = standby::Role::from_env(&std::env::var("LOGGER_ROLE").unwrap_or_default());
    let instance = instance_key();
    let host = std::env::var("MC_HOST").unwrap_or_else(|_| "purityvanilla.com".to_owned());
    let port: i32 = std::env::var("MC_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(25565);

    // The protocol spoken *to the server*, not the one azalea was built with.
    let target = std::env::var("MC_TARGET_VERSION").unwrap_or_else(|_| "1.21.10".to_owned());

    let db = match Db::connect(&database_url).await {
        Ok(db) => db,
        Err(error) => {
            eprintln!("cannot reach Postgres: {error}");
            eprintln!("Check DATABASE_URL, and that the server is running.");
            std::process::exit(1);
        }
    };

    // Opt-in and destructive: drops every table this bot owns.
    if std::env::var("RESET_DB").map(|v| v == "1" || v == "true").unwrap_or(false) {
        log("RESET_DB set — dropping and recreating all tables");
        if let Err(error) = db.reset().await {
            eprintln!("reset failed: {error}");
            std::process::exit(1);
        }
        log("database reset");
    } else if let Err(error) = db.migrate().await {
        eprintln!("migration failed: {error:#}");
        std::process::exit(1);
    }

    // A heartbeat row from our own previous run is not evidence of a live
    // logger — it is what we left behind when we were killed. Clear it before
    // the sweep below, which deliberately spares sessions that are still being
    // heartbeated on (a standby's, or the other instance's).
    if let Err(error) = db.clear_heartbeat(&instance).await {
        eprintln!("could not clear the previous heartbeat: {error}");
    }

    match db.close_stale_sessions().await {
        Ok(n) if n > 0 => log(&format!("closed {n} session row(s) left open by a previous run")),
        Ok(_) => {}
        Err(error) => eprintln!("could not close stale sessions: {error}"),
    }

    log(&format!("{} death patterns loaded", classify::death_matcher_count()));

    // Opened before connecting: events can arrive within milliseconds of login,
    // and without a session id they would be written unattributed.
    let session_id = match db.start_session(&host, port, &target, None).await {
        Ok(id) => id,
        Err(error) => {
            eprintln!("could not open session row: {error}");
            std::process::exit(1);
        }
    };
    log(&format!("session #{session_id} opened"));

    log(&format!("signing in as {email}"));
    let account = match Account::microsoft(&email).await {
        Ok(account) => account,
        Err(error) => {
            eprintln!("Microsoft sign-in failed: {error}");
            std::process::exit(1);
        }
    };

    // ViaProxy is only needed when the server speaks a version azalea does not.
    // Setting MC_TARGET_VERSION=native|none|direct connects without ViaProxy.
    let use_via = !matches!(target.as_str(), "native" | "none" | "direct");

    let db = Arc::new(db);
    // Filled in when a gateway is configured: Discord's lines arrive here.
    let mut say_rx: Option<tokio::sync::mpsc::UnboundedReceiver<mc_stream::Say>> = None;
    let stream = match std::env::var("EVENT_STREAM_ADDR") {
        Ok(addr) if !addr.trim().is_empty() => {
            let key = std::env::var("SERVER_KEY")
                .ok()
                .map(|k| k.trim().to_owned())
                .filter(|k| !k.is_empty())
                .unwrap_or_else(|| env_file_key().unwrap_or_else(|| "default".into()));
            log(&format!("event stream producer → {addr} (server={key})"));
            let (publisher, says) =
                mc_stream::StreamPublisher::spawn_with_says(addr, key, mc_stream::token_from_env());
            say_rx = Some(says);
            Some(publisher)
        }
        _ => None,
    };
    let _ = SERVICES.set(Services {
        // A standby starts gated: it logs in, sits in-game, and writes only
        // once the standby loop sees the primary's heartbeat go stale.
        writer: Arc::clone(&db).spawn_writer(host.clone(), stream, !role.is_standby()),
        db: Arc::clone(&db),
        session_id: std::sync::atomic::AtomicI64::new(session_id),
        logins: std::sync::atomic::AtomicU32::new(0),
        host: host.clone(),
        port,
        target_version: target.clone(),
        enter_portal: std::env::var("ENTER_PORTAL")
            .map(|v| v == "1" || v == "true")
            .unwrap_or(false),
        portal_walking: std::sync::atomic::AtomicBool::new(false),
        in_main_world: std::sync::atomic::AtomicBool::new(false),
        anti_afk: if std::env::var("ANTI_AFK").map(|v| v == "1" || v == "true").unwrap_or(false) {
            let min = std::env::var("ANTI_AFK_MIN_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(45);
            let max = std::env::var("ANTI_AFK_MAX_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(120);
            Some((min, max.max(min)))
        } else {
            None
        },
        anti_afk_started: std::sync::atomic::AtomicBool::new(false),
        connected: std::sync::atomic::AtomicBool::new(false),
        login_at_ms: std::sync::atomic::AtomicI64::new(0),
        plugin_probe: Mutex::new(PluginProbeState::default()),
        queue_probe: std::env::var("QUEUE_PROBE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false),
        queue_leave_at: std::env::var("QUEUE_LEAVE_AT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1)
            .max(1),
        queue_leaving: std::sync::atomic::AtomicBool::new(false),
        in_queue_limbo: std::sync::atomic::AtomicBool::new(false),
        connection_msgs: {
            std::env::var("CONNECTION_MSGS")
                .map(|v| {
                    let v = v.trim();
                    v == "1" || v.eq_ignore_ascii_case("true")
                })
                .unwrap_or_else(|_| host.to_lowercase().contains("6b6t"))
        },
        tablist_joins: match std::env::var("TABLIST_JOINS") {
            Ok(v) => {
                let v = v.trim();
                if v == "1" || v.eq_ignore_ascii_case("true") {
                    true
                } else if v == "0" || v.eq_ignore_ascii_case("false") {
                    false
                } else {
                    let conn = std::env::var("CONNECTION_MSGS")
                        .map(|c| {
                            let c = c.trim();
                            c == "1" || c.eq_ignore_ascii_case("true")
                        })
                        .unwrap_or_else(|_| host.to_lowercase().contains("6b6t"));
                    !conn
                }
            }
            _ => {
                // Default off when chat connection messages are expected (6b6t).
                let conn = std::env::var("CONNECTION_MSGS")
                    .map(|v| {
                        let v = v.trim();
                        v == "1" || v.eq_ignore_ascii_case("true")
                    })
                    .unwrap_or_else(|_| host.to_lowercase().contains("6b6t"));
                !conn
            }
        },
        connection_msgs_sent: std::sync::atomic::AtomicBool::new(false),
        startup_commands: std::env::var("STARTUP_COMMANDS")
            .unwrap_or_default()
            .split(',')
            .map(|c| c.trim().to_owned())
            .filter(|c| !c.is_empty())
            .collect(),
        startup_commands_sent: std::sync::atomic::AtomicBool::new(false),
        bot_username: Mutex::new(None),
        bot_handle: Mutex::new(None),
        say_enabled: std::env::var("SAY_ENABLED")
            .map(|v| {
                let v = v.trim();
                !(v == "0" || v.eq_ignore_ascii_case("false"))
            })
            .unwrap_or(true),
        // A Discord line starting with "/" would run a command as this account.
        // Off unless someone deliberately turns it on.
        say_allow_commands: std::env::var("SAY_ALLOW_COMMANDS")
            .map(|v| {
                let v = v.trim();
                v == "1" || v.eq_ignore_ascii_case("true")
            })
            .unwrap_or(false),
        say_prefix: std::env::var("SAY_PREFIX").unwrap_or_default(),
        lobby_only: std::env::var("LOBBY_ONLY")
            .map(|v| {
                let v = v.trim();
                v == "1" || v.eq_ignore_ascii_case("true")
            })
            .unwrap_or(false),
        lobby_escape_leaving: std::sync::atomic::AtomicBool::new(false),
        tab_online: Mutex::new(HashMap::new()),
        tab_hidden: Mutex::new(HashSet::new()),
        tab_reconcile_ticks: std::sync::atomic::AtomicU32::new(0),
        settle_last_len: std::sync::atomic::AtomicUsize::new(0),
        settle_quiet_ticks: std::sync::atomic::AtomicU32::new(0),
        settle_done: std::sync::atomic::AtomicBool::new(false),
        reported_online: std::sync::atomic::AtomicI64::new(-1),
    });
    // Rejoins missed while the bot was not watching. Periodic rather than
    // hung off an event: the cases that lose joins -- a restart, a kick, a
    // spell in the lobby, a join announcement lost in the flood -- have no
    // single moment worth waking on, and the pass costs one indexed query
    // over whoever is on the tab list. It writes nothing unless someone is
    // sitting on a leave while visibly online, so running it forever is
    // cheap and self-correcting.
    {
        let every = std::env::var("RECONCILE_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300u64)
            .max(60);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(every)).await;
                start_online_reconcile(services(), 0);
            }
        });
    }

    // Heartbeat (every logger) and takeover watch (standby only).
    {
        let takeover = std::env::var("STANDBY_TAKEOVER_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(45u64)
            .max(15);
        let interval = std::env::var("HEARTBEAT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(15u64)
            .clamp(5, takeover / 2);
        if role.is_standby() {
            log(&format!(
                "standby mode (instance \"{instance}\") — in-game but not logging while a primary \
                 heartbeats; taking over after {takeover}s of silence"
            ));
        } else {
            log(&format!("primary logger (instance \"{instance}\")"));
        }
        standby::spawn(
            Arc::clone(&db),
            standby::Config {
                instance: instance.clone(),
                role,
                host: host.clone(),
                takeover_after: std::time::Duration::from_secs(takeover),
                interval: std::time::Duration::from_secs(interval),
            },
            services().writer.gate(),
            || {
                let services = services();
                standby::Snapshot {
                    connected: services.connected.load(std::sync::atomic::Ordering::Acquire),
                    session_id: services.session(),
                    bot_username: services
                        .bot_username
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .clone(),
                }
            },
        );
    }

    if let Some(says) = say_rx {
        if services().say_enabled {
            log("Discord relay on: /say from Discord will be spoken in game");
            tokio::spawn(relay_discord_says(says));
        } else {
            log("Discord relay off (SAY_ENABLED=false); incoming lines are dropped");
        }
    }

    if services().queue_probe {
        log(&format!(
            "queue probe on — leave at position ≤ {}",
            services().queue_leave_at
        ));
    }
    if services().connection_msgs {
        log("will enable /connectionmsgs so join/leave come from chat (not tab list)");
    }
    if !services().tablist_joins {
        log("storing join/leave from chat only; tab list is presence-only");
    }
    // Ctrl+C would otherwise leave ended_at NULL, which is exactly the stale-row
    // problem close_stale_sessions has to clean up on the next start.
    {
        let db = Arc::clone(&db);
        let instance = instance.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                log("shutting down");
                // The live session, which is not the one opened at startup once
                // the client has reconnected at least once.
                let _ = db.end_session(services().session(), "shutdown: interrupted").await;
                // Let a standby take over now rather than after the staleness
                // window, since we know we are going away.
                let _ = db.clear_heartbeat(&instance).await;
                std::process::exit(0);
            }
        });
    }

    log(&format!("connecting to {host}:{port}"));

    if use_via {
        // Downloads ViaProxy to ~/.minecraft/azalea-viaversion on first run and
        // starts it in the background. Requires a Java 17+ runtime on PATH.
        log(&format!("starting ViaProxy targeting {target}"));
        let via = ViaVersionPlugin::start(&target).await;
        ClientBuilder::new_without_plugins()
            .add_plugins(azalea::DefaultPlugins)
            .add_plugins(DefaultBotPlugins)
            .add_plugins(via)
            .set_handler(handle)
            .start(account, host.as_str())
            .await
    } else {
        log("connecting directly (no ViaProxy)");
        ClientBuilder::new_without_plugins()
            .add_plugins(azalea::DefaultPlugins)
            .add_plugins(DefaultBotPlugins)
            .set_handler(handle)
            .start(account, host.as_str())
            .await
    }
}

async fn handle(bot: Client, event: Event, _state: State) -> eyre::Result<()> {
    let services = services();

    match event {
        Event::Login => {
            services.connected.store(true, std::sync::atomic::Ordering::Release);
            services
                .login_at_ms
                .store(Utc::now().timestamp_millis(), std::sync::atomic::Ordering::Relaxed);
            services
                .queue_leaving
                .store(false, std::sync::atomic::Ordering::Release);
            services
                .connection_msgs_sent
                .store(false, std::sync::atomic::Ordering::Release);
            services
                .startup_commands_sent
                .store(false, std::sync::atomic::Ordering::Release);
            clear_tab_presence(services);
            log("logged in");

            {
                let mut probe = services.plugin_probe.lock().unwrap_or_else(|e| e.into_inner());
                probe.reset();
                probe.probe_start_ms = now_ms();
            }

            // Tell the server we accept common plugin channels — Bukkit sends its
            // own `minecraft:register` list back on join; registering first also
            // prompts some plugins to send channel payloads we can sniff.
            bot.write_packet(ServerboundCustomPayload {
                identifier: Identifier::new("minecraft:register"),
                data: UnsizedByteArray(
                    b"worldedit:cui\0bungeecord:main\0luckperms:update\0".to_vec(),
                ),
            });

            // A reconnect: the previous session was ended by the disconnect, so
            // everything from here belongs to a new one. Without this the rest of
            // the run is attributed to a closed session and counts as offline.
            if services.logins.fetch_add(1, std::sync::atomic::Ordering::Relaxed) > 0 {
                match services
                    .db
                    .start_session(
                        &services.host,
                        services.port,
                        &services.target_version,
                        Some(&bot.username()),
                    )
                    .await
                {
                    Ok(id) => {
                        services.session_id.store(id, std::sync::atomic::Ordering::Relaxed);
                        log(&format!("session #{id} opened after reconnect"));
                    }
                    Err(error) => eprintln!("could not open session after reconnect: {error}"),
                }
            }
        }

        Event::Spawn => {
            *services.bot_handle.lock().unwrap_or_else(|e| e.into_inner()) = Some(bot.clone());
            let username = bot.username();
            log(&format!("spawned as {username}"));
            {
                *services.bot_username.lock().unwrap_or_else(|e| e.into_inner()) = Some(username.clone());
            }
            if let Err(error) = services.db.set_bot_username(services.session(), &username).await {
                eprintln!("could not record bot username: {error}");
            }

            // Probe timing starts at Login (register channels arrive before spawn).
            if let Ok(mut probe) = services.plugin_probe.lock() {
                if probe.probe_start_ms == 0 {
                    probe.probe_start_ms = now_ms();
                }
            }

            // Queue limbo also fires Spawn — do not disconnect here. Leave only when
            // position hits QUEUE_LEAVE_AT (or we see an in-game "Connected" line).
            if services.queue_probe {
                return Ok(());
            }

            if !services.enter_portal {
                services
                    .in_main_world
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            start_portal_walk(&bot, services);
            start_anti_afk(&bot, services);
            enable_connection_msgs(&bot, services);
            run_startup_commands(&bot, services);
            start_lobby_stay_guard(&bot, services);
        }

        Event::Tick => {
            plugin_probe_tick(&bot, services).await;
            maybe_reconcile_tab_presence(&bot, services);
            if services.enter_portal {
                if let Ok(pos) = bot.position() {
                    // Lobby dump / void: keep jumping so we don't soft-lock underwater/void.
                    if pos.y < 40.0 {
                        bot.jump();
                    }
                }
            }
        }

        Event::Packet(packet) => {
            handle_probe_packet(&bot, services, &packet);
            handle_queue_packet(&bot, services, &packet).await;
            handle_tab_presence_packet(services, &packet);
        }

        Event::Chat(packet) => {
            let text = packet.message().to_string().to_lowercase();
            if text.contains("backup server") || text.contains("bottom of the world") {
                start_portal_walk(&bot, services);
            }
            enforce_lobby_stay(&bot, services, &text);
            if services.queue_probe {
                handle_queue_probe(&bot, services, &packet).await;
            } else {
                let plain = packet.message().to_string();
                note_queue_limbo(services, &plain).await;
                if plain.to_lowercase().contains("connected to the server") {
                    leave_queue_limbo(&bot, services).await;
                }
            }
            // Queue probe / limbo must not feed the Discord chat bridge.
            if !suppress_public_feed(services) {
                handle_chat(&packet, services);
            }
        }

        // The tab list is the authoritative source for who is online; chat only
        // reports it when the server bothers to announce it.
        Event::AddPlayer(info) => {
            presence_note_join(services, info.profile.uuid, &info.profile.name);
        }
        Event::RemovePlayer(info) => {
            presence_note_leave(services, info.profile.uuid);
        }

        Event::Death(_) => log("the bot died"),

        Event::Disconnect(reason) => {
            services.connected.store(false, std::sync::atomic::Ordering::Release);
            *services.bot_handle.lock().unwrap_or_else(|e| e.into_inner()) = None;
            services.portal_walking.store(false, std::sync::atomic::Ordering::Release);
            services.in_main_world.store(false, std::sync::atomic::Ordering::Release);
            let text = reason.map(|r| r.to_string()).unwrap_or_else(|| "unknown".to_owned());
            log(&format!("disconnected: {text}"));
            clear_tab_presence(services);
            if let Err(error) = services
                .db
                .end_session(services.session(), &format!("disconnected: {text}"))
                .await
            {
                eprintln!("could not close session: {error}");
            }

            // Azalea keeps retrying in-process. After a Microsoft auth rate
            // limit that retry can hang for tens of minutes with no chat.
            // Exit so systemd starts a fresh process after RestartSec.
            let wait_secs = std::env::var("RECONNECT_EXIT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(180);
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(wait_secs)).await;
                if let Some(svc) = SERVICES.get() {
                    if !svc.connected.load(std::sync::atomic::Ordering::Acquire) {
                        log(&format!(
                            "still offline after {wait_secs}s — exiting for systemd restart"
                        ));
                        std::process::exit(1);
                    }
                }
            });
        }

        _ => {}
    }

    Ok(())
}

/// `Position in queue: 328` (2b2t / MatsuQueue and lookalikes).
fn parse_queue_position(text: &str) -> Option<i32> {
    static RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(
            r"(?i)(?:position\s+in\s+queue|queue\s*position|in\s+queue)\s*[:=]?\s*#?\s*(\d+)",
        )
        .expect("queue regex")
    });
    RE.captures(text)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse().ok())
}

fn looks_like_queue_limbo(text: &str) -> bool {
    let lower = text.to_lowercase();
    parse_queue_position(text).is_some()
        || lower.contains("position in queue")
        || lower.contains("2b2t is full")
        || lower.contains("queued for server")
}

fn suppress_public_feed(services: &Services) -> bool {
    services.queue_probe
        || services
            .in_queue_limbo
            .load(std::sync::atomic::Ordering::Acquire)
        // Lobby tab list on a server that needs a portal walk (6b6t) is not
        // the real player list — without this, the brief window between
        // Spawn and the walk finishing let a handful of lobby occupants get
        // recorded as the server's online presence.
        || (services.enter_portal
            && !services
                .in_main_world
                .load(std::sync::atomic::Ordering::Acquire))
}

/// Main logger: while waiting in queue, close the session so the queue tablist
/// does not count as online, and pause chat writes for the Discord bridge.
async fn note_queue_limbo(services: &Services, text: &str) {
    if services.queue_probe || !looks_like_queue_limbo(text) {
        return;
    }
    if services
        .in_queue_limbo
        .swap(true, std::sync::atomic::Ordering::AcqRel)
    {
        return;
    }
    log("queue limbo — pausing chat bridge + presence until connected");
    clear_tab_presence(services);
    if let Err(err) = services
        .db
        .end_session(services.session(), "queue limbo")
        .await
    {
        eprintln!("could not close session for queue limbo: {err}");
    }
}

async fn leave_queue_limbo(bot: &Client, services: &Services) {
    if services.queue_probe {
        return;
    }
    if !services
        .in_queue_limbo
        .swap(false, std::sync::atomic::Ordering::AcqRel)
    {
        return;
    }
    log("left queue limbo — opening a fresh session for the main server");
    match services
        .db
        .start_session(
            &services.host,
            services.port,
            &services.target_version,
            Some(&bot.username()),
        )
        .await
    {
        Ok(id) => {
            services
                .session_id
                .store(id, std::sync::atomic::Ordering::Relaxed);
            log(&format!("session #{id} opened (post-queue)"));
        }
        Err(err) => eprintln!("could not open post-queue session: {err}"),
    }
}

async fn handle_queue_probe(bot: &Client, services: &Services, packet: &ChatPacket) {
    let plain = packet.message().to_string();
    // Custom-font queue digits often stringify to whitespace; dump JSON once.
    if plain.chars().filter(|c| !c.is_whitespace()).count() < 8 {
        if let Ok(raw) = serde_json::to_string(&packet.message()) {
            if raw.contains("font") || raw.contains("extra") {
                static DUMPED: std::sync::atomic::AtomicBool =
                    std::sync::atomic::AtomicBool::new(false);
                if !DUMPED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                    let preview: String = raw.chars().take(800).collect();
                    log(&format!("queue chat raw sample: {preview}"));
                }
            }
        }
    }
    apply_queue_probe(bot, services, &plain).await;
}

/// Pulls a live player count out of a tab-list header/footer, e.g.
/// "Online players: 504" or "587 players online". Tries both orders since
/// different servers phrase it differently; returns the first match, since a
/// header can contain other unrelated numbers (ping, TPS, a rank).
fn parse_online_count(text: &str) -> Option<i64> {
    let lower = text.to_lowercase();
    // Accept a "," thousands separator (e.g. "3,921") alongside the plain
    // digit run — stripped before parsing since i64::from_str rejects it.
    if let Some(pos) = lower.find("online players") {
        let after = &lower[pos + "online players".len()..];
        let digits: String = after
            .trim_start_matches(|c: char| !c.is_ascii_digit() && c != '\n')
            .chars()
            .take_while(|c| c.is_ascii_digit() || *c == ',')
            .filter(|c| *c != ',')
            .collect();
        if let Ok(n) = digits.parse() {
            return Some(n);
        }
    }
    if let Some(pos) = lower.find("players online") {
        let before = lower[..pos].trim_end();
        let digits: String = before
            .chars()
            .rev()
            .take_while(|c| c.is_ascii_digit() || *c == ',')
            .filter(|c| *c != ',')
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        if let Ok(n) = digits.parse() {
            return Some(n);
        }
    }
    None
}

#[cfg(test)]
mod reported_online_tests {
    use super::parse_online_count;

    #[test]
    fn reads_6b6t_style_header() {
        assert_eq!(
            parse_online_count("Ping: 97   Rank: [Prime]\nOnline players: 504\n"),
            Some(504)
        );
    }

    #[test]
    fn reads_2b2t_style_header() {
        assert_eq!(
            parse_online_count("18.61 tps — 587 players online — 88 ping"),
            Some(587)
        );
    }

    #[test]
    fn ignores_unrelated_digits() {
        assert_eq!(parse_online_count("Ping: 97   Rank: [Prime]"), None);
    }

    #[test]
    fn handles_no_count_present() {
        assert_eq!(parse_online_count("welcome to the server"), None);
    }

    #[test]
    fn strips_thousands_separator() {
        assert_eq!(
            parse_online_count("Online players: 3,921\n"),
            Some(3921)
        );
        assert_eq!(
            parse_online_count("18.61 tps — 12,587 players online — 88 ping"),
            Some(12587)
        );
    }
}

async fn handle_queue_packet(bot: &Client, services: &Services, packet: &ClientboundGamePacket) {
    use azalea::protocol::packets::game::c_boss_event::Operation;

    let text = match packet {
        ClientboundGamePacket::TabList(p) => {
            let text = format!("{}\n{}", p.header, p.footer);
            // The server already knows and broadcasts its own player count in
            // this same header/footer text — independent of, and far faster
            // than, the PlayerInfoUpdate roster this connection otherwise has
            // to reconstruct player-by-player. Not scoped to lobby vs. main
            // world: it is the server's own whole-network figure, so it is
            // written as soon as it is seen rather than waiting on that.
            if let Some(count) = parse_online_count(&text) {
                let prev = services
                    .reported_online
                    .swap(count, std::sync::atomic::Ordering::Relaxed);
                // The Discord bot only trusts this value while
                // reported_online_at is within its own 30s window, so a
                // stable count still needs a periodic write to keep that
                // timestamp fresh -- otherwise it silently falls back to a
                // much slower roster count during exactly the steady
                // stretches this figure is supposed to help with. The same
                // periodic write also retries a count that failed to persist
                // earlier, since it isn't gated on the value having changed.
                static LAST_WRITE: std::sync::Mutex<Option<std::time::Instant>> =
                    std::sync::Mutex::new(None);
                let stale = LAST_WRITE.lock().is_ok_and(|guard| match *guard {
                    Some(t) => t.elapsed() >= std::time::Duration::from_secs(20),
                    None => true,
                });
                if prev != count || stale {
                    if let Ok(mut last) = LAST_WRITE.lock() {
                        *last = Some(std::time::Instant::now());
                    }
                    let db = services.db.clone();
                    let instance = instance_key();
                    tokio::spawn(async move {
                        if let Err(err) = db.set_reported_online(&instance, count as i32).await {
                            eprintln!("could not write reported online count: {err}");
                        }
                    });
                }
            }
            if text.chars().any(|c| c.is_ascii_digit()) || text.to_lowercase().contains("queue")
            {
                static LAST: std::sync::Mutex<String> = std::sync::Mutex::new(String::new());
                if let Ok(mut last) = LAST.lock() {
                    if *last != text {
                        *last = text.clone();
                        let preview: String = text.chars().take(240).collect();
                        log(&format!("queue tablist: {preview}"));
                    }
                }
            }
            text
        }
        ClientboundGamePacket::SetActionBarText(p) => p.text.to_string(),
        ClientboundGamePacket::BossEvent(p) => match &p.operation {
            Operation::Add(add) => add.name.to_string(),
            Operation::UpdateName(name) => name.to_string(),
            _ => return,
        },
        _ => return,
    };

    if services.queue_probe {
        apply_queue_probe(bot, services, &text).await;
    } else {
        note_queue_limbo(services, &text).await;
        let lower = text.to_lowercase();
        if lower.contains("connected to the server") {
            leave_queue_limbo(bot, services).await;
        }
    }
}

async fn apply_queue_probe(bot: &Client, services: &Services, plain: &str) {
    let lower = plain.to_lowercase();

    // Slipped past the queue into the actual server — cycle back out.
    if lower.contains("connected to the server")
        && !services
            .queue_leaving
            .swap(true, std::sync::atomic::Ordering::AcqRel)
    {
        log("queue probe: reached the server — leaving to rejoin queue");
        let _ = services
            .db
            .upsert_queue_status(Some(0), plain, false)
            .await;
        bot.disconnect();
        return;
    }

    let Some(position) = parse_queue_position(plain) else {
        return;
    };

    log(&format!("queue position: {position}"));
    if let Err(err) = services
        .db
        .upsert_queue_status(Some(position), plain, true)
        .await
    {
        eprintln!("queue status write failed: {err}");
    }

    if position <= services.queue_leave_at
        && !services
            .queue_leaving
            .swap(true, std::sync::atomic::Ordering::AcqRel)
    {
        log(&format!(
            "queue probe: position {position} ≤ {} — leaving to rejoin",
            services.queue_leave_at
        ));
        bot.disconnect();
    }
}

fn is_own_bot(services: &Services, name: &str) -> bool {
    services
        .bot_username
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_deref()
        .is_some_and(|bot| bot.eq_ignore_ascii_case(name))
}

/// Countdown value at which everyone is marked offline. The servers here all
/// count down to 1, so that is the default; `RESTART_OFFLINE_SECS` raises it
/// for a server that jumps straight from 10 to gone.
fn restart_offline_secs() -> u64 {
    static SECS: std::sync::LazyLock<u64> = std::sync::LazyLock::new(|| {
        std::env::var("RESTART_OFFLINE_SECS")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(1)
    });
    *SECS
}

fn handle_chat(packet: &ChatPacket, services: &Services) {
    let component = packet.message();
    let plain_text = component.to_string();
    let ansi = component.to_ansi();

    if let Ok(mut probe) = services.plugin_probe.lock() {
        if probe.waiting_chat {
            let found = plugins_from_chat(&plain_text);
            if !found.is_empty() {
                probe.chat_plugins.extend(found);
                probe.methods.insert(PluginSource::Chat.as_str().to_owned());
                probe.waiting_chat = false;
            }
        }
    }

    let (source, is_player_packet) = match packet {
        ChatPacket::Player(_) => ("p", true),
        ChatPacket::System(_) => ("s", false),
        ChatPacket::Disguised(_) => ("d", false),
    };

    let result = classify::classify(&plain_text, is_player_packet, packet.is_whisper(), None);

    // For a player packet the sender is real. For a system line it has to be
    // recovered from the text, since most servers reformat chat before sending
    // it and the packet carries no sender at all.
    let sender_name = if is_player_packet { packet.sender() } else { result.sender.clone() };
    let sender_name = sender_name.and_then(|name| classify::clean_player_name(&name));
    let content = if is_player_packet { Some(packet.content()) } else { result.content.clone() };
    let sender_label = result.sender_label.clone().or_else(|| {
        content
            .as_deref()
            .and_then(|body| classify::label_before_content(&plain_text, body))
    });
    // Don't store a label that is identical to the plain name.
    let sender_label = sender_label.filter(|label| {
        sender_name
            .as_ref()
            .map(|name| label != name)
            .unwrap_or(true)
    });

    println!("{} {ansi}", stamp());

    let plugin = sender_name.as_deref().is_some_and(classify::is_plugin_speaker);
    let spam = classify::is_bridge_spam(&plain_text)
        || content.as_deref().is_some_and(classify::is_bridge_spam)
        || matches!(result.kind, Kind::Unknown);
    let real_player = is_player_packet
        && sender_name
            .as_deref()
            .is_some_and(|name| !classify::is_plugin_speaker(name));
    if ((spam || plugin) && !real_player) || (is_player_packet && sender_name.is_none()) {
        return;
    }

    // Join/leave live only in player_events (Discord synthesizes feed lines).
    // Everything else still gets a chat_messages row.
    if !matches!(result.kind, Kind::Join | Kind::Leave) {
        // Chat/whisper: body only. Death: empty (Discord rebuilds from names).
        // Advancement: trailing [title] only. Server/unknown: keep the line.
        let store_plain = match result.kind {
            Kind::Chat | Kind::Whisper => content.clone().unwrap_or_else(|| plain_text.clone()),
            Kind::Death => String::new(),
            Kind::Advancement => match (plain_text.rfind('['), plain_text.rfind(']')) {
                (Some(start), Some(end)) if end > start => plain_text[start..=end].to_string(),
                _ => String::new(),
            },
            _ => plain_text.clone(),
        };

        services.writer.chat(ChatRow {
            session_id: services.session(),
            received_at: Utc::now(),
            source: source.to_owned(),
            kind: result.kind.as_str().to_owned(),
            sender_name: sender_name.clone(),
            sender_uuid: packet.sender_uuid(),
            subject_name: result.subject.clone(),
            killer_name: result.killer.clone(),
            sender_label,
            plain_text: store_plain,
            ansi: Some(ansi.clone()),
        });
    }

    // A restart countdown that has run out. The server drops every player at
    // once and does not send a tab-list removal for each one on the way down,
    // so without this the online list stays full until the bot reconnects.
    // Only system lines are considered, so a player typing about restarts
    // cannot clear the tab list.
    if !is_player_packet {
        if let Some(secs) = classify::restart_countdown_secs(&plain_text) {
            if secs <= restart_offline_secs() {
                let cleared = {
                    let mut online =
                        services.tab_online.lock().unwrap_or_else(|e| e.into_inner());
                    let n = online.len();
                    online.clear();
                    n
                };
                if cleared > 0 {
                    log(&format!(
                        "restart in {secs}s — marking {cleared} player(s) offline"
                    ));
                }
                services
                    .writer
                    .server_restart(services.session(), Utc::now());
                // No reconcile is scheduled here on purpose. The bot is not
                // usually disconnected by this -- 6b6t restarts the proxy and
                // puts us back in the lobby -- so the tab list does not refill
                // until the portal walk has run again, on no schedule worth
                // guessing at. The periodic pass picks it up once it has.
            }
        }
    }

    // Join/leave announced in chat. Skip when the tab list already stores them.
    if matches!(result.kind, Kind::Join | Kind::Leave) && !services.tablist_joins {
        if let Some(name) = result.subject.filter(|name| !is_own_bot(services, name)) {
            services.writer.event(PlayerEventRow {
                session_id: services.session(),
                occurred_at: Utc::now(),
                event_type: result.kind.as_str().to_owned(),
                source: "c".to_owned(),
                player_name: name,
                player_uuid: None,
                ansi: Some(ansi.clone()),
            });
        }
    }
}

fn record_player_event(
    services: &Services,
    event_type: &str,
    source: &str,
    player_name: String,
    player_uuid: Option<Uuid>,
) {
    services.writer.event(PlayerEventRow {
        session_id: services.session(),
        occurred_at: Utc::now(),
        event_type: event_type.to_owned(),
        source: source.to_owned(),
        player_name,
        player_uuid,
        // Presence from the tab list; the server never printed a line for it.
        ansi: None,
    });
}

fn clear_tab_presence(services: &Services) {
    services
        .tab_online
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clear();
    services
        .tab_hidden
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clear();
    // Whatever tab list rebuilds next (post-login, post-reconnect, or
    // post-queue) is a fresh flood — let the settle watch fire again for it.
    services
        .settle_last_len
        .store(0, std::sync::atomic::Ordering::Relaxed);
    services
        .settle_quiet_ticks
        .store(0, std::sync::atomic::Ordering::Relaxed);
    services
        .settle_done
        .store(false, std::sync::atomic::Ordering::Relaxed);
}

/// Once the post-login tab flood has settled, hand the list to the writer so
/// joins that happened while the bot was away can be filled in.
///
/// Waiting is the whole point: the tab list arrives as a flood of AddPlayer
/// over several seconds, and a list read halfway through is a list missing
/// people. The wait is deliberately longer than the window that decides
/// snapshot-versus-join, so by the time this runs the classification for this
/// login has already been made and written.
///
/// Only what the bot can see is reported. Players absent from the tab list are
/// left alone rather than assumed gone -- absence from a list that may still
/// be loading is not evidence of a leave, and guessing there would invent
/// hundreds of them.
fn start_online_reconcile(services: &'static Services, delay_ms: u64) {
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        if suppress_public_feed(services) {
            return;
        }
        let players: Vec<String> = {
            let online = services.tab_online.lock().unwrap_or_else(|e| e.into_inner());
            online
                .values()
                .filter(|name| !is_own_bot(services, name))
                .cloned()
                .collect()
        };
        if players.is_empty() {
            return;
        }
        log(&format!(
            "reconciling {} online player(s) against the log",
            players.len()
        ));
        services
            .writer
            .reconcile_online(services.session(), Utc::now(), players);
    });
}

fn presence_note_join(services: &Services, uuid: Uuid, name: &str) {
    let Some(name) = classify::clean_player_name(name) else {
        return;
    };
    if suppress_public_feed(services) || name.is_empty() {
        return;
    }
    if services
        .tab_hidden
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(&uuid)
    {
        return;
    }
    {
        let mut online = services.tab_online.lock().unwrap_or_else(|e| e.into_inner());
        if online.insert(uuid, name.clone()).is_some() {
            return;
        }
    }
    let login_at = services.login_at_ms.load(std::sync::atomic::Ordering::Relaxed);
    let elapsed = Utc::now().timestamp_millis() - login_at;
    // Chat-owned servers (6b6t) keep tab rows as snapshots so online_now
    // still works without counting a second join. The logger's own username
    // is never posted as a public join.
    let event_type = if is_own_bot(services, &name)
        || !services.tablist_joins
        || login_at == 0
        || elapsed < SNAPSHOT_GRACE_MS
    {
        "s"
    } else {
        "j"
    };
    record_player_event(services, event_type, "t", name, Some(uuid));
}

fn presence_note_leave(services: &Services, uuid: Uuid) {
    let name = {
        let mut online = services.tab_online.lock().unwrap_or_else(|e| e.into_inner());
        online.remove(&uuid)
    };
    let Some(name) = name else {
        return;
    };
    if suppress_public_feed(services) {
        return;
    }
    // `p` = tab presence leave; stats and Discord only count `l` / chat leaves.
    let event_type = if is_own_bot(services, &name) || !services.tablist_joins {
        "p"
    } else {
        "l"
    };
    record_player_event(services, event_type, "t", name, Some(uuid));
}

fn handle_tab_presence_packet(services: &Services, packet: &ClientboundGamePacket) {
    match packet {
        ClientboundGamePacket::PlayerInfoUpdate(p) => {
            for entry in &p.entries {
                let uuid = entry.profile.uuid;
                if p.actions.update_listed {
                    if entry.listed {
                        services
                            .tab_hidden
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .remove(&uuid);
                        if !entry.profile.name.is_empty() {
                            presence_note_join(services, uuid, &entry.profile.name);
                        }
                    } else {
                        services
                            .tab_hidden
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .insert(uuid);
                        presence_note_leave(services, uuid);
                    }
                } else if p.actions.add_player && !entry.profile.name.is_empty() {
                    presence_note_join(services, uuid, &entry.profile.name);
                }
            }
        }
        ClientboundGamePacket::PlayerInfoRemove(p) => {
            for uuid in &p.profile_ids {
                services
                    .tab_hidden
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(uuid);
                presence_note_leave(services, *uuid);
            }
        }
        _ => {}
    }
}

fn maybe_reconcile_tab_presence(bot: &Client, services: &Services) {
    let tick = services
        .tab_reconcile_ticks
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if tick % TAB_RECONCILE_TICKS != 0 {
        return;
    }
    if suppress_public_feed(services) {
        return;
    }
    let Ok(tab) = bot.tab_list() else {
        return;
    };
    let (gone, tracked): (Vec<Uuid>, usize) = {
        let online = services.tab_online.lock().unwrap_or_else(|e| e.into_inner());
        (
            online
                .keys()
                .copied()
                .filter(|uuid| !tab.contains_key(uuid))
                .collect(),
            online.len(),
        )
    };
    // Diagnostic for the online-count drift seen on 6b6t: shows whether
    // azalea's own tracked roster (`tab.len()`) has itself drifted from
    // reality, or whether the mismatch is only in our bookkeeping — logged
    // roughly once a minute rather than every ~5s reconcile tick.
    if tick % (TAB_RECONCILE_TICKS * 12) == 0 {
        log(&format!(
            "tab presence check: azalea tab_list={} tracked={} retiring={}",
            tab.len(),
            tracked,
            gone.len()
        ));
    }
    for uuid in gone {
        presence_note_leave(services, uuid);
    }

    // Adaptive settle detection: a fixed delay before reconciling the tab
    // list against the log guessed wrong in both directions — too short on
    // 6b6t (a 4-minute guess still only caught 333 of 478 real players
    // during testing) and pure waste on a small server that settles in
    // seconds. Watching for growth to actually stop is neither: it fires as
    // soon as the flood is done, however long that took.
    if !services.settle_done.load(std::sync::atomic::Ordering::Relaxed) {
        let prev = services
            .settle_last_len
            .swap(tracked, std::sync::atomic::Ordering::Relaxed);
        if tracked > 0 && tracked == prev {
            let quiet = services
                .settle_quiet_ticks
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1;
            if quiet >= SETTLE_QUIET_TICKS {
                services
                    .settle_done
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                log(&format!(
                    "tab list settled at {tracked} players ({}s quiet) — reconciling",
                    quiet * (TAB_RECONCILE_TICKS / 20)
                ));
                start_online_reconcile(crate::services(), 0);
            }
        } else {
            services
                .settle_quiet_ticks
                .store(0, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

/// `.env.6b6t` → `6b6t`, for both the stream key and the instance name.
fn env_file_key() -> Option<String> {
    let path = std::env::var("ENV_FILE").ok()?;
    let name = std::path::Path::new(&path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(&path);
    name.strip_prefix(".env.")
        .or_else(|| name.strip_prefix("env."))
        .map(|s| s.to_owned())
        .filter(|s| !s.is_empty())
}

/// Unique name for *this process* in `logger_heartbeats`.
///
/// Deliberately not `SERVER_KEY`: a standby shares the server key with the
/// primary it backs up (Discord subscribes by that key), so the two are told
/// apart by their env file — `.env.6b6t` and `.env.6b6t-backup` — or by an
/// explicit `LOGGER_ID`.
fn instance_key() -> String {
    if let Ok(id) = std::env::var("LOGGER_ID") {
        let id = id.trim();
        if !id.is_empty() {
            return id.to_owned();
        }
    }
    env_file_key()
        .or_else(|| {
            std::env::var("SERVER_KEY")
                .ok()
                .map(|k| k.trim().to_owned())
                .filter(|k| !k.is_empty())
        })
        .unwrap_or_else(|| "default".into())
}

fn require_env(name: &str, description: &str) -> String {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => value,
        _ => {
            eprintln!("Set {name} to {description}.");
            std::process::exit(1);
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn handle_probe_packet(bot: &Client, services: &Services, packet: &ClientboundGamePacket) {
    let mut probe = match services.plugin_probe.lock() {
        Ok(probe) => probe,
        Err(_) => return,
    };
    if probe.saved {
        return;
    }

    match packet {
        ClientboundGamePacket::Commands(commands) => {
            let names = commands.entries.iter().filter_map(|node| node.name());
            let (tree, alias) = plugins_from_command_names(names);
            if !tree.is_empty() {
                probe.tree_plugins.extend(tree);
                probe.methods.insert(PluginSource::CommandTree.as_str().to_owned());
            }
            if alias.is_some() {
                probe.version_alias = alias;
            }
            drop(probe);
            maybe_request_tab_complete(bot, services);
        }
        ClientboundGamePacket::CommandSuggestions(suggestions) => {
            if probe.pending_tab_id != Some(suggestions.id) {
                return;
            }
            let texts = suggestions
                .suggestions
                .list()
                .iter()
                .map(|s| s.text())
                .collect::<Vec<_>>();
            let tab = plugins_from_tab_suggestions(texts);
            if !tab.is_empty() {
                probe.tab_plugins.extend(tab);
                probe.methods.insert(PluginSource::TabComplete.as_str().to_owned());
            }
            probe.pending_tab_id = None;
        }
        ClientboundGamePacket::CustomPayload(payload) => {
            let channel = payload.identifier.to_string();
            let path = payload.identifier.path();

            if path == "brand" {
                let mut cursor = std::io::Cursor::new(payload.data.0.as_slice());
                if let Ok(brand) = String::azalea_read(&mut cursor) {
                    probe.server_brand = Some(brand);
                }
            } else if path == "register" {
                let channels = parse_register_channels(&payload.data.0);
                let found = plugins_from_register_channels(&channels);
                if !found.is_empty() {
                    probe.register_plugins.extend(found);
                    probe
                        .methods
                        .insert(PluginSource::RegisterChannel.as_str().to_owned());
                }
            } else if path == "unregister" {
                let channels = parse_register_channels(&payload.data.0);
                for name in plugins_from_register_channels(&channels) {
                    probe.register_plugins.remove(&name);
                }
            } else if let Some(name) = plugin_from_channel(&channel) {
                probe.channel_plugins.insert(name);
                probe
                    .methods
                    .insert(PluginSource::PluginChannel.as_str().to_owned());
            }
        }
        _ => {}
    }
}

fn maybe_request_tab_complete(bot: &Client, services: &Services) {
    let mut probe = match services.plugin_probe.lock() {
        Ok(probe) => probe,
        Err(_) => return,
    };
    if probe.tab_requested {
        return;
    }
    let Some(alias) = probe.version_alias.clone() else {
        return;
    };
    let id = (now_ms() as u32) ^ 0xA5A5_0000;
    probe.pending_tab_id = Some(id);
    probe.tab_requested = true;
    drop(probe);

    bot.write_packet(ServerboundCommandSuggestion {
        id,
        command: format!("{alias} "),
    });
}

async fn plugin_probe_tick(bot: &Client, services: &Services) {
    let snapshot = {
        let mut probe = match services.plugin_probe.lock() {
            Ok(probe) => probe,
            Err(_) => return,
        };
        if probe.saved || probe.probe_start_ms == 0 {
            return;
        }

        probe.ticks_since_start += 1;

        if !probe.chat_requested && probe.ticks_since_start == 60 {
            probe.chat_requested = true;
            let chat_probe = std::env::var("PLUGIN_PROBE_CHAT")
                .map(|v| v == "1" || v == "true")
                .unwrap_or(false);
            if chat_probe {
                probe.waiting_chat = true;
                bot.chat("/plugins");
            }
        }

        if probe.waiting_chat && probe.ticks_since_start == 160 {
            bot.chat("/pl");
        }

        if probe.ticks_since_start >= 300 {
            probe.waiting_chat = false;
            Some(std::mem::take(&mut *probe))
        } else {
            if !probe.tab_requested && probe.ticks_since_start == 20 && probe.version_alias.is_some() {
                drop(probe);
                maybe_request_tab_complete(bot, services);
            }
            None
        }
    };

    if let Some(snapshot) = snapshot {
        save_plugin_probe(services, snapshot).await;
    }
}

async fn save_plugin_probe(services: &Services, probe: PluginProbeState) {
    if probe.saved {
        return;
    }

    let sources = [
        (PluginSource::CommandTree, probe.tree_plugins.into_iter().collect()),
        (PluginSource::TabComplete, probe.tab_plugins.into_iter().collect()),
        (PluginSource::Chat, probe.chat_plugins.into_iter().collect()),
        (
            PluginSource::RegisterChannel,
            probe.register_plugins.into_iter().collect(),
        ),
        (
            PluginSource::PluginChannel,
            probe.channel_plugins.into_iter().collect(),
        ),
    ];
    let merged = merge_plugins(&sources);
    let methods: Vec<String> = probe.methods.into_iter().collect();
    let plugins_json = serde_json::Value::Array(
        merged
            .iter()
            .map(|entry| {
                serde_json::json!({
                    "name": entry.name,
                    "sources": entry.sources.iter().collect::<Vec<_>>(),
                })
            })
            .collect(),
    );

    let count = merged.len();
    let notes = if count == 0 {
        Some(
            "No plugins detected — server likely hides command tree, tab-complete, /plugins, and register channels."
                .to_owned(),
        )
    } else {
        None
    };

    if let Err(error) = services
        .db
        .save_plugin_scan(
            services.session(),
            &services.host,
            probe.server_brand.as_deref(),
            &methods,
            &plugins_json,
            notes.as_deref(),
        )
        .await
    {
        eprintln!("could not save plugin scan: {error}");
        return;
    }

    let methods_label = if methods.is_empty() {
        "none".to_owned()
    } else {
        methods.join(", ")
    };
    log(&format!("plugin probe saved ({count} plugins via {methods_label})"));

    if let Ok(mut state) = services.plugin_probe.lock() {
        state.saved = true;
    }
}

/// Lobby AFK logger: if 6b6t dumps us onto a survival worker, leave so we can
/// rejoin the actual lobby (presence/chat would otherwise double the main bot).
fn enforce_lobby_stay(bot: &Client, services: &Services, lower_chat: &str) {
    if !services.lobby_only || services.enter_portal || services.queue_probe {
        return;
    }
    let dumped = (lower_chat.contains("playing on") && lower_chat.contains("worker"))
        || lower_chat.contains("you're now playing on worker");
    if !dumped {
        return;
    }
    leave_main_for_lobby(bot, services, "chat said we are on a survival worker");
}

fn start_lobby_stay_guard(bot: &Client, services: &Services) {
    if !services.lobby_only || services.enter_portal || services.queue_probe {
        return;
    }
    services
        .lobby_escape_leaving
        .store(false, std::sync::atomic::Ordering::Release);
    let bot = bot.clone();
    tokio::spawn(async move {
        // Give spawn / dimension switch a moment before judging by Y.
        tokio::time::sleep(std::time::Duration::from_secs(12)).await;
        let Some(svc) = SERVICES.get() else {
            return;
        };
        if !svc.lobby_only || svc.enter_portal || svc.queue_probe {
            return;
        }
        if let Ok(pos) = bot.position() {
            // Lobby sits high (~y160); survival spawn is normal overworld Y.
            if (50.0..135.0).contains(&pos.y) && find_lobby_portal(&bot).is_none() {
                leave_main_for_lobby(&bot, svc, &format!("y={:.0} looks like main world", pos.y));
            }
        }
    });
}

fn leave_main_for_lobby(bot: &Client, services: &Services, why: &str) {
    if services
        .lobby_escape_leaving
        .swap(true, std::sync::atomic::Ordering::AcqRel)
    {
        return;
    }
    log(&format!(
        "lobby-only: {why} — disconnecting to rejoin the lobby"
    ));
    bot.disconnect();
}

fn start_portal_walk(bot: &Client, services: &Services) {
    if !services.enter_portal {
        return;
    }
    if services
        .portal_walking
        .swap(true, std::sync::atomic::Ordering::SeqCst)
    {
        return;
    }
    let bot = bot.clone();
    tokio::spawn(async move {
        let left_lobby = walk_into_portal(bot).await;
        if let Some(svc) = SERVICES.get() {
            svc.portal_walking
                .store(false, std::sync::atomic::Ordering::Release);
            // Only mark the bot as past the lobby once it's actually
            // confirmed to have left — a timeout or a give-up after repeated
            // approaches still leaves it looking at the lobby's own (much
            // smaller) tab list, and treating that as the real roster writes
            // false joins and a bogus online count.
            if left_lobby {
                svc.in_main_world
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            // Reconcile fires from the adaptive settle watch in
            // maybe_reconcile_tab_presence once the survival tab list
            // actually stops growing, not on a guessed delay from here.
        }
    });
}

fn start_anti_afk(bot: &Client, services: &Services) {
    let Some((min, max)) = services.anti_afk else {
        return;
    };
    if services
        .anti_afk_started
        .swap(true, std::sync::atomic::Ordering::SeqCst)
    {
        return;
    }
    log(&format!("anti-afk on: an action every {min}-{max}s"));
    let bot = bot.clone();
    tokio::spawn(async move { anti_afk(bot, min, max).await });
}

/// Speaks lines that arrived from Discord.
///
/// Everything here is a guard on someone else's keyboard reaching this account:
/// commands are refused unless explicitly allowed, control characters and
/// colour codes are stripped, the line is capped at Minecraft's own limit, and
/// a minimum gap keeps a busy channel from tripping the server's spam kick.
async fn relay_discord_says(mut says: tokio::sync::mpsc::UnboundedReceiver<mc_stream::Say>) {
    const MAX_CHARS: usize = 256;
    let gap = std::time::Duration::from_millis(
        std::env::var("SAY_MIN_GAP_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1500),
    );
    let mut last_sent = std::time::Instant::now() - gap;

    while let Some(say) = says.recv().await {
        let services = services();
        let who = say.from.as_deref().unwrap_or("discord");

        // One line only: a newline would be a second chat packet.
        let text: String = say
            .text
            .chars()
            .filter(|c| !c.is_control() && *c != '§')
            .collect();
        let text = text.trim();
        if text.is_empty() {
            continue;
        }
        if text.starts_with('/') && !services.say_allow_commands {
            log(&format!("refusing command from {who}: {text}"));
            continue;
        }

        let mut line = format!("{}{who}: {text}", services.say_prefix);
        if line.chars().count() > MAX_CHARS {
            line = line.chars().take(MAX_CHARS).collect();
        }

        let elapsed = last_sent.elapsed();
        if elapsed < gap {
            tokio::time::sleep(gap - elapsed).await;
        }

        let client = services
            .bot_handle
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        match client {
            Some(bot) => {
                log(&format!("→ game: {line}"));
                bot.chat(&line);
                last_sent = std::time::Instant::now();
            }
            None => log(&format!("dropping line from {who}: not in game right now")),
        }
    }
}

/// 6b6t: `/connectionmsgs on` shows join/leave in chat (free command).
fn enable_connection_msgs(bot: &Client, services: &Services) {
    if !services.connection_msgs {
        return;
    }
    if services
        .connection_msgs_sent
        .swap(true, std::sync::atomic::Ordering::SeqCst)
    {
        return;
    }
    let bot = bot.clone();
    tokio::spawn(async move {
        // Wait until we're past lobby auth / portal dump spam.
        tokio::time::sleep(std::time::Duration::from_secs(8)).await;
        log("sending /connectionmsgs on");
        bot.chat("/connectionmsgs on");
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        // Re-assert after portal / backup dumps which sometimes reset toggles.
        bot.chat("/connectionmsgs on");
    });
}

/// Commands from `STARTUP_COMMANDS`, once per login, for state only the server
/// can set (`/showspam on`) or answer (`/rules`). Replies arrive as ordinary
/// chat and are logged.
///
/// Each command goes exactly once per session. Repeating one is left to the
/// operator rather than done here, because whether that is safe depends on the
/// command: `/showspam on` names the state it wants and can be sent twice, but
/// a bare toggle would flip straight back off. List it twice to re-assert it.
fn run_startup_commands(bot: &Client, services: &Services) {
    if services.startup_commands.is_empty() {
        return;
    }
    if services
        .startup_commands_sent
        .swap(true, std::sync::atomic::Ordering::SeqCst)
    {
        return;
    }
    let bot = bot.clone();
    let commands = services.startup_commands.clone();
    tokio::spawn(async move {
        // Past lobby auth, the portal walk, and the second /connectionmsgs
        // send at t=10s -- two chat packets in one instant look like spam to
        // the very filter this is usually used to turn off.
        tokio::time::sleep(std::time::Duration::from_secs(14)).await;
        for command in commands {
            log(&format!("sending startup command: {command}"));
            bot.chat(&command);
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
    });
}

async fn anti_afk(bot: Client, min_secs: u64, max_secs: u64) {
    use azalea::entity::LookDirection;
    use rand::Rng;

    loop {
        let delay = rand::rng().random_range(min_secs..=max_secs);
        tokio::time::sleep(std::time::Duration::from_secs(delay)).await;

        let action = rand::rng().random_range(0..3);
        match action {
            0 => bot.jump(),
            1 => {
                let _ = bot.set_crouching(true);
                tokio::time::sleep(std::time::Duration::from_millis(400)).await;
                let _ = bot.set_crouching(false);
            }
            _ => {
                let current = bot
                    .component::<LookDirection>()
                    .map(|d| (d.y_rot(), d.x_rot()))
                    .unwrap_or((0.0, 0.0));
                let mut rng = rand::rng();
                let yaw = current.0 + rng.random_range(-30.0..30.0_f32);
                let pitch = (current.1 + rng.random_range(-10.0..10.0_f32)).clamp(-90.0, 90.0);
                let _ = bot.set_direction(yaw, pitch);
            }
        }
    }
}

/// How far from the lobby portal counts as "we actually teleported".
const LEFT_LOBBY_BLOCKS: f64 = 24.0;

/// 6b6t lobby portal sits around y=163. Survival portals are usually near ground.
fn is_lobby_portal(pos: azalea::BlockPos) -> bool {
    pos.y >= 140 && pos.y <= 190
}

/// How long to keep hunting for a lobby portal.
///
/// 6b6t's backup dump says "just a minute" while it reserves a spot — giving up
/// after 30s leaves the bot falling in the void forever.
fn portal_search_secs(bot: &Client) -> u32 {
    match bot.position() {
        Ok(pos) if pos.y >= 130.0 || pos.y < 50.0 => 180,
        _ => 25,
    }
}

/// Returns whether the bot is confirmed to have left the lobby (or was
/// already past it) — `false` means it's still stuck there, so callers must
/// not treat this as having reached the main world.
async fn walk_into_portal(bot: Client) -> bool {
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Already on the main world (normal ground Y, no lobby portal nearby)?
    if let Ok(pos) = bot.position() {
        if (50.0..130.0).contains(&pos.y) && find_lobby_portal(&bot).is_none() {
            log("portal: looks like main server already — skipping");
            return true;
        }
    }

    log("portal: searching for lobby portal…");
    let mut waited = 0u32;
    let mut last_log = 0u32;
    let portal = loop {
        let budget = portal_search_secs(&bot).max(waited + 1);
        if let Some(found) = find_lobby_portal(&bot) {
            break found;
        }
        if let Ok(pos) = bot.position() {
            if pos.y < 50.0 {
                bot.jump();
            }
            if waited == 0 || waited.saturating_sub(last_log) >= 15 {
                last_log = waited;
                log(&format!(
                    "portal: still waiting (y={:.0}, {}s / {}s)",
                    pos.y, waited, budget
                ));
            }
        }
        waited += 1;
        if waited >= budget {
            log("portal: none found yet — will retry on next lobby/backup message");
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    };

    let approach = bot.position().unwrap_or(portal.center());
    log(&format!("portal: found at {portal}, walking into it"));

    // Keep trying until we leave the lobby — do not soft-lock after one miss.
    for attempt in 1..=12u32 {
        if left_lobby(&bot, portal) {
            log("portal: left the lobby");
            return true;
        }

        log(&format!("portal: approach attempt {attempt}"));
        goto_block(&bot, portal, 30).await;

        if wait_to_leave_lobby(&bot, portal, 25).await {
            log("portal: teleported");
            return true;
        }

        let outside = step_away_from_portal(approach, portal);
        log(&format!(
            "portal: still in the lobby, stepping out to {outside} and back in"
        ));
        goto_block(&bot, outside, 15).await;
    }
    log("portal: gave up after repeated approaches — waiting for next dump message");
    false
}

fn find_lobby_portal(bot: &Client) -> Option<azalea::BlockPos> {
    let position = bot.position().ok()?;
    let world = bot.world().ok()?;
    let world = world.read();

    let mut best_lobby: Option<(f64, azalea::BlockPos)> = None;
    let mut best_any: Option<(f64, azalea::BlockPos)> = None;

    for block in world.find_blocks(position, &BlockKind::NetherPortal.into()) {
        let dist = position.distance_to(block.center());
        if dist > 128.0 {
            continue;
        }
        if is_lobby_portal(block) {
            if best_lobby.map_or(true, |(d, _)| dist < d) {
                best_lobby = Some((dist, block));
            }
        } else if best_any.map_or(true, |(d, _)| dist < d) {
            best_any = Some((dist, block));
        }
    }

    best_lobby.or(best_any).map(|(_, pos)| pos)
}

fn left_lobby(bot: &Client, portal: azalea::BlockPos) -> bool {
    bot.position()
        .ok()
        .is_some_and(|pos| pos.distance_to(portal.center()) > LEFT_LOBBY_BLOCKS)
}

async fn wait_to_leave_lobby(bot: &Client, portal: azalea::BlockPos, secs: u64) -> bool {
    for _ in 0..secs {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        if left_lobby(bot, portal) {
            return true;
        }
        if let Ok(pos) = bot.position() {
            if pos.y < 50.0 {
                bot.jump();
            }
        }
    }
    false
}

fn step_away_from_portal(from: azalea::Vec3, portal: azalea::BlockPos) -> azalea::BlockPos {
    let center = portal.center();
    let dx = from.x - center.x;
    let dz = from.z - center.z;
    if dx.abs() >= dz.abs() && dx.abs() > 0.25 {
        azalea::BlockPos::new(
            portal.x + if dx >= 0.0 { 3 } else { -3 },
            portal.y,
            portal.z,
        )
    } else if dz.abs() > 0.25 {
        azalea::BlockPos::new(
            portal.x,
            portal.y,
            portal.z + if dz >= 0.0 { 3 } else { -3 },
        )
    } else {
        azalea::BlockPos::new(portal.x + 3, portal.y, portal.z)
    }
}

async fn goto_block(bot: &Client, target: azalea::BlockPos, timeout_secs: u64) {
    tokio::select! {
        _ = bot.goto(BlockPosGoal(target)) => {}
        _ = tokio::time::sleep(std::time::Duration::from_secs(timeout_secs)) => {
            bot.stop_pathfinding();
        }
    }
}

fn stamp() -> String {
    chrono::Local::now().format("%H:%M:%S").to_string()
}

fn log(message: &str) {
    println!("{} [bot] {message}", stamp());
}
