use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use poise::serenity_prelude as serenity;
use serenity::builder::CreateMessage;
use serenity::model::id::ChannelId;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::config::{BridgeStyle, Config, ServerConfig};
use crate::database::{presence_kind, ChatRow, Db, EventRoute, ROUTE_KINDS};
use crate::discord::topic::{ChannelTopic, TopicResult};
use crate::presentation::embeds::{
    build_event_embed, build_feed_batch_embed, build_feed_link_embed, pack_feed, FeedLink,
};
use crate::presentation::heads;

const PRESENCE_DEDUP: Duration = Duration::from_secs(20);
const WATCHBRIDGE_HITS_PER_TICK: i64 = 200;
const DISCORD_RETRY_ATTEMPTS: u8 = 8;
const DISCORD_RETRY_FALLBACK: Duration = Duration::from_secs(2);

fn route_channel(
    row: &ChatRow,
    fallback: &str,
    routes: &[EventRoute],
    default_kinds: &[String],
) -> Option<String> {
    let kind = crate::database::canonical_route_kind(&row.kind)?;
    if let Some(route) = routes.iter().find(|r| r.kind == kind) {
        if !route.enabled {
            return None;
        }
        return Some(
            route
                .channel_id
                .clone()
                .unwrap_or_else(|| fallback.to_string()),
        );
    }
    if crate::database::EventKind::matches_name(default_kinds, &row.kind) {
        Some(fallback.to_string())
    } else {
        None
    }
}

struct BridgeInner {
    cursor: i64,
    cursor_at: Option<chrono::DateTime<chrono::Utc>>,
    event_cursor: i64,
    event_cursor_at: Option<chrono::DateTime<chrono::Utc>>,
    presence_cursor: i64,
    presence_cursor_at: Option<chrono::DateTime<chrono::Utc>>,
    last_post_at: Option<chrono::DateTime<chrono::Utc>>,
    recent_watchbridge: HashMap<String, Instant>,
    recent_presence: HashMap<String, Instant>,
    topic: ChannelTopic,
}

pub struct Bridge {
    http: Arc<serenity::http::Http>,
    server: ServerConfig,
    config: Arc<Config>,
    db: Arc<Db>,
    inner: RwLock<BridgeInner>,
    stopped: AtomicBool,
    handles: Mutex<Vec<JoinHandle<()>>>,
    /// Live gateway-link state, shared with the stream subscriber. `None` until
    /// the bridge starts, or when this server has no gateway configured.
    link: Mutex<Option<Arc<AtomicBool>>>,
    /// Sends Discord lines back down the gateway to the logger. `None` when this
    /// server has no gateway — the reverse path only exists over the stream.
    say: Mutex<Option<mc_stream::SayHandle>>,
}

impl Bridge {
    pub fn new(
        http: Arc<serenity::http::Http>,
        server: ServerConfig,
        config: Arc<Config>,
        db: Arc<Db>,
    ) -> Self {
        let topic = ChannelTopic::new(http.clone(), server.clone(), config.clone(), db.clone());
        Self {
            http,
            server,
            config,
            db,
            inner: RwLock::new(BridgeInner {
                cursor: 0,
                cursor_at: None,
                event_cursor: 0,
                event_cursor_at: None,
                presence_cursor: 0,
                presence_cursor_at: None,
                last_post_at: None,
                recent_watchbridge: HashMap::new(),
                recent_presence: HashMap::new(),
                topic,
            }),
            stopped: AtomicBool::new(false),
            handles: Mutex::new(Vec::new()),
            link: Mutex::new(None),
            say: Mutex::new(None),
        }
    }

    pub fn key(&self) -> &str {
        &self.server.key
    }

    pub async fn current_cursor(&self) -> i64 {
        self.inner.read().await.cursor
    }

    pub fn uses_stream(&self) -> bool {
        self.server.stream_addr.is_some()
    }

    pub async fn start(self: &Arc<Self>) -> eyre::Result<()> {
        if !self.handles.lock().is_empty() {
            return Ok(());
        }
        self.db.ensure_bridge_state(&self.server.key).await?;

        {
            let mut inner = self.inner.write().await;
            let (stored_id, stored_at) = self.db.get_cursor(&self.server.key).await?;
            if stored_id > 0 {
                inner.cursor = stored_id;
                inner.cursor_at = stored_at;
            } else if self.config.bridge.start_from_latest {
                let (id, at) = self.db.latest_chat_cursor(&self.server.key).await?;
                inner.cursor = id;
                inner.cursor_at = at;
                self.db
                    .set_cursor(&self.server.key, inner.cursor, inner.cursor_at)
                    .await?;
            } else {
                inner.cursor = 0;
                inner.cursor_at = None;
                self.db.set_cursor(&self.server.key, 0, None).await?;
            }

            let (stored_event, stored_event_at) =
                self.db.get_event_cursor(&self.server.key).await?;
            if stored_event > 0 {
                inner.event_cursor = stored_event;
                inner.event_cursor_at = stored_event_at;
            } else {
                let (id, at) = self.db.latest_player_event_cursor(&self.server.key).await?;
                inner.event_cursor = id;
                inner.event_cursor_at = at;
            }
            self.db
                .set_event_cursor(&self.server.key, inner.event_cursor, inner.event_cursor_at)
                .await?;

            let (stored_presence, stored_presence_at) =
                self.db.get_presence_cursor(&self.server.key).await?;
            if stored_presence > 0 {
                inner.presence_cursor = stored_presence;
                inner.presence_cursor_at = stored_presence_at;
            } else {
                let (id, at) = self.db.latest_presence_cursor(&self.server.key).await?;
                inner.presence_cursor = id;
                inner.presence_cursor_at = at;
            }
            self.db
                .set_presence_cursor(
                    &self.server.key,
                    inner.presence_cursor,
                    inner.presence_cursor_at,
                )
                .await?;

            let settings = self.db.get_bridge_settings(&self.server.key).await?;
            if self.server.key == self.config.servers[0].key
                && settings.channel_id.is_none()
                && settings.enabled
            {
                if let Some(ref ch) = self.config.discord.bridge_channel_id {
                    self.db
                        .set_bridge_channel(&self.server.key, Some(ch))
                        .await?;
                    tracing::info!(
                        "[bridge:{}] seeded channel from .env: {ch}",
                        self.server.key
                    );
                }
            }

            // Style and rainbow are per-server DB settings now (`/chatbridge
            // customize`), so this reflects whatever was last saved rather than
            // the .env default — it can go stale the moment an admin changes it
            // live, but it's only ever printed once at startup.
            let style_note = if settings.rainbow {
                format!("{} + rainbow", settings.style.as_str())
            } else {
                settings.style.as_str().to_string()
            };
            if let Some(ref addr) = self.server.stream_addr {
                tracing::info!(
                    "[bridge:{}] live feed via stream {addr} ({style_note} style)",
                    self.server.key
                );
            } else {
                tracing::info!(
                    "[bridge:{}] live feed via DB poll from message id {} ({style_note} style)",
                    self.server.key,
                    inner.cursor
                );
            }
        }

        let mut handles = Vec::new();

        // Side loop: watch alerts + channel topic (+ DB poll feed when no stream).
        let this = Arc::clone(self);
        let poll = Duration::from_millis(self.config.bridge.poll_ms);
        handles.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(poll);
            loop {
                interval.tick().await;
                if this.stopped.load(Ordering::SeqCst) {
                    break;
                }
                if let Err(err) = this.tick().await {
                    tracing::error!("[bridge:{}] tick failed: {err}", this.server.key);
                }
            }
        }));

        if let Some(addr) = self.server.stream_addr.clone() {
            let this = Arc::clone(self);
            let server_key = self.server.key.clone();
            // Replay recent buffered events on (re)connect; skip already-seen seqs.
            let mut sub = mc_stream::StreamSubscriber::spawn_with(
                addr.clone(),
                server_key.clone(),
                mc_stream::SubscribeOptions {
                    replay: 200,
                    client: Some("mc-discord-bot".into()),
                    ..Default::default()
                },
            );
            let link = sub.connection_flag();
            *self.link.lock() = Some(Arc::clone(&link));
            *self.say.lock() = Some(sub.say_handle());
            handles.push(tokio::spawn(async move {
                let mut last_seq: u64 = 0;
                let cap = this
                    .config
                    .bridge
                    .embeds_per_message
                    .max(this.config.bridge.lines_per_message)
                    .max(1);
                while let Some(first) = sub.rx.recv().await {
                    if this.stopped.load(Ordering::SeqCst) {
                        break;
                    }
                    let mut events = Vec::with_capacity(cap);
                    if let Some(event) = accept_stream_event(&mut last_seq, first) {
                        events.push(event);
                    }
                    while events.len() < cap {
                        match sub.rx.try_recv() {
                            Ok(event) => {
                                if let Some(event) = accept_stream_event(&mut last_seq, event) {
                                    events.push(event);
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    if events.is_empty() {
                        continue;
                    }
                    if let Err(err) = this.on_stream_events(events).await {
                        tracing::error!("[bridge:{server_key}] stream deliver failed: {err}");
                    }
                }
            }));

            // Tell the channel whether the live link is actually up, on startup
            // and on every change after that. Without it a dead gateway looks
            // exactly like a quiet server.
            if self.config.bridge.status_notices {
                let this = Arc::clone(self);
                handles.push(tokio::spawn(async move {
                    this.watch_feed_link(link).await;
                }));
            }
        } else if self.config.bridge.status_notices {
            let this = Arc::clone(self);
            let poll_ms = self.config.bridge.poll_ms;
            handles.push(tokio::spawn(async move {
                this.post_feed_notice(
                    FeedLink::Polling,
                    &format!(
                        "No event gateway configured for this server, so the feed reads new rows \
                         from the database every {:.1}s. Set `EVENT_STREAM_ADDR` to stream instead.",
                        poll_ms as f64 / 1000.0
                    ),
                )
                .await;
            }));
        }

        *self.handles.lock() = handles;
        Ok(())
    }

    /// Startup notice plus up/down transitions for the gateway link.
    ///
    /// Deliberately never names the gateway address in these — they post to a
    /// feed channel every server member can read, and `127.0.0.1:9700` is
    /// meaningless (or worse, a distraction) to anyone who isn't running the
    /// stack. An admin debugging a dead feed already has `mc-tail --status`.
    async fn watch_feed_link(self: Arc<Self>, link: Arc<AtomicBool>) {
        const CHECK: Duration = Duration::from_secs(5);
        const STARTUP_GRACE: Duration = Duration::from_secs(15);
        /// Consecutive misses before announcing a loss, so a reconnect that
        /// takes two seconds does not post anything at all.
        const LOSS_AFTER: u32 = 3;

        let started = Instant::now();
        while started.elapsed() < STARTUP_GRACE && !link.load(Ordering::Relaxed) {
            if self.stopped.load(Ordering::SeqCst) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        let mut up = link.load(Ordering::Relaxed);
        if up {
            self.post_feed_notice(
                FeedLink::Streaming,
                &format!(
                    "Connected to the event gateway for **{}**.",
                    self.server.label
                ),
            )
            .await;
        } else {
            self.post_feed_notice(
                FeedLink::GatewayDown,
                &format!(
                    "Cannot reach the event gateway for **{}** — retrying every 2s. \
                     Nothing will appear here until the link is back.\n\
                     An admin can check it with `mc-tail --status`, or `systemctl status terminal-client`.",
                    self.server.label
                ),
            )
            .await;
        }

        let mut misses = 0u32;
        loop {
            tokio::time::sleep(CHECK).await;
            if self.stopped.load(Ordering::SeqCst) {
                break;
            }
            if link.load(Ordering::Relaxed) {
                misses = 0;
                if !up {
                    up = true;
                    self.post_feed_notice(
                        FeedLink::Recovered,
                        &format!(
                            "Back on the gateway for **{}**. Buffered events were replayed.",
                            self.server.label
                        ),
                    )
                    .await;
                }
            } else if up {
                misses += 1;
                if misses >= LOSS_AFTER {
                    up = false;
                    misses = 0;
                    self.post_feed_notice(
                        FeedLink::GatewayDown,
                        &format!(
                            "Lost the connection to the event gateway for **{}**. \
                             Reconnecting every 2s; the feed resumes where it left off.",
                            self.server.label
                        ),
                    )
                    .await;
                }
            }
        }
    }

    /// Speak a line in game. `Err` explains why it could not be sent, so the
    /// command can say so rather than silently swallowing it.
    pub fn say(&self, text: &str, from: &str) -> Result<(), &'static str> {
        let handle = self.say.lock().clone();
        let Some(handle) = handle else {
            return Err("this server has no event gateway, so there is no way back into the game");
        };
        if self.gateway_up() != Some(true) {
            return Err("the gateway link is down right now — try again once the feed reconnects");
        }
        if handle.say(text, Some(from.to_string())) {
            Ok(())
        } else {
            Err("the gateway connection is gone")
        }
    }

    /// True when this server's gateway link is currently up. `None` when the
    /// server has no gateway configured and reads the database instead.
    pub fn gateway_up(&self) -> Option<bool> {
        self.link
            .lock()
            .as_ref()
            .map(|flag| flag.load(Ordering::Relaxed))
    }

    /// Announce that the bot is back on Discord, and say whether the live feed
    /// survived the outage. Called after a full gateway reconnect, never on the
    /// first connect — that one already gets the startup notice.
    pub async fn announce_bot_reconnect(&self) {
        if !self.config.bridge.status_notices || self.stopped.load(Ordering::SeqCst) {
            return;
        }
        let detail = match (self.gateway_up(), self.server.stream_addr.as_deref()) {
            (Some(true), Some(addr)) => format!(
                "Back online. The live feed stayed connected to the gateway at `{addr}`, \
                 and buffered events were replayed."
            ),
            (Some(false), Some(addr)) => format!(
                "Back online, but the event gateway at `{addr}` is not reachable — retrying. \
                 Check `mc-tail --status`."
            ),
            _ => format!(
                "Back online. This feed reads the database every {:.1}s.",
                self.config.bridge.poll_ms as f64 / 1000.0
            ),
        };
        self.post_feed_notice(FeedLink::BotReconnected, &detail)
            .await;
    }

    /// Posts into the bridge channel, unless the feed is unset or paused.
    async fn post_feed_notice(&self, link: FeedLink, detail: &str) {
        let settings = match self.db.get_bridge_settings(&self.server.key).await {
            Ok(settings) => settings,
            Err(err) => {
                tracing::warn!(
                    "[bridge:{}] cannot read bridge settings for a feed notice: {err}",
                    self.server.key
                );
                return;
            }
        };
        if !settings.enabled {
            return;
        }
        let Some(channel_id) = settings.channel_id else {
            return;
        };
        let embed = build_feed_link_embed(&self.server.label, link, detail);
        match self.send_embeds(&channel_id, vec![embed]).await {
            Ok(()) => tracing::info!(
                "[bridge:{}] posted {link:?} notice to channel {channel_id}",
                self.server.key
            ),
            Err(err) => tracing::warn!(
                "[bridge:{}] feed notice to {channel_id} failed: {err}",
                self.server.key
            ),
        }
    }

    pub async fn skip_to_latest(&self) -> eyre::Result<()> {
        let (latest, at) = self.db.latest_chat_cursor(&self.server.key).await?;
        let (presence, presence_at) = self.db.latest_presence_cursor(&self.server.key).await?;
        {
            let mut inner = self.inner.write().await;
            inner.cursor = latest;
            inner.cursor_at = at;
            inner.presence_cursor = presence;
            inner.presence_cursor_at = presence_at;
        }
        self.db.set_cursor(&self.server.key, latest, at).await?;
        self.db
            .set_presence_cursor(&self.server.key, presence, presence_at)
            .await?;
        Ok(())
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        for handle in self.handles.lock().drain(..) {
            handle.abort();
        }
    }

    pub async fn post_test_line(&self) -> eyre::Result<bool> {
        let settings = self.db.get_bridge_settings(&self.server.key).await?;
        let Some(channel_id) = settings.channel_id else {
            return Ok(false);
        };
        let sample = ChatRow {
            id: 0,
            received_at: chrono::Utc::now(),
            kind: "server".into(),
            sender_name: None,
            sender_label: None,
            subject_name: None,
            killer_name: None,
            plain_text: format!(
                "Feed test from {} — this is what a line looks like.",
                self.server.label
            ),
            server_host: None,
        };
        let embed = match settings.style {
            BridgeStyle::Rich => {
                let head = head_subject(&sample).and_then(|n| heads::url_for(n, &self.config));
                build_event_embed(&sample, head.as_deref(), settings.rainbow)
            }
            BridgeStyle::Compact => {
                build_feed_batch_embed(&[sample], &self.server.label, settings.rainbow)
            }
        };
        match self.send_embeds(&channel_id, vec![embed]).await {
            Ok(()) => Ok(true),
            Err(err) => {
                tracing::error!("[bridge:{}] test post failed: {err}", self.server.key);
                Ok(false)
            }
        }
    }

    pub async fn refresh_topic(&self) -> TopicResult {
        let settings = self.db.get_bridge_settings(&self.server.key).await.ok();
        let channel_id = settings.and_then(|s| s.channel_id);
        self.inner
            .write()
            .await
            .topic
            .update(channel_id.as_deref())
            .await
    }

    async fn tick(&self) -> eyre::Result<()> {
        let settings = self.db.get_bridge_settings(&self.server.key).await?;
        // Stream mode owns the live feed; DB poll is fallback only.
        if self.server.stream_addr.is_none() {
            self.deliver_feed(&settings).await?;
        }
        self.deliver_watchbridge(&settings).await?;
        self.inner
            .write()
            .await
            .topic
            .maybe_update(settings.channel_id.as_deref())
            .await;
        Ok(())
    }

    async fn on_stream_events(&self, events: Vec<mc_stream::StreamEvent>) -> eyre::Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let settings = self.db.get_bridge_settings(&self.server.key).await?;
        let mut rows = Vec::with_capacity(events.len());
        let mut latest_chat: Option<(i64, chrono::DateTime<chrono::Utc>)> = None;
        for event in events {
            match event {
                mc_stream::StreamEvent::Chat(chat) => {
                    let id = chat.id.unwrap_or(0);
                    let ts = chat.ts;
                    rows.push(ChatRow {
                        id,
                        received_at: ts,
                        kind: chat.kind,
                        sender_name: chat.sender_name,
                        sender_label: chat.sender_label,
                        subject_name: chat.subject_name,
                        killer_name: chat.killer_name,
                        plain_text: chat.plain_text,
                        server_host: chat.server_host,
                    });
                    if id > 0 {
                        match latest_chat {
                            Some((prev, _)) if id <= prev => {}
                            _ => latest_chat = Some((id, ts)),
                        }
                    }
                }
                mc_stream::StreamEvent::PlayerEvent(ev) => {
                    let Some(kind) = presence_kind(&ev.event_type) else {
                        continue;
                    };
                    rows.push(ChatRow {
                        id: 0,
                        received_at: ev.ts,
                        kind: kind.into(),
                        sender_name: None,
                        sender_label: None,
                        subject_name: Some(ev.player_name),
                        killer_name: None,
                        plain_text: String::new(),
                        server_host: ev.server_host,
                    });
                }
            }
        }
        self.deliver_routed_rows(rows, &settings).await?;
        if let Some((id, ts)) = latest_chat {
            let mut inner = self.inner.write().await;
            if id > inner.cursor {
                inner.cursor = id;
                inner.cursor_at = Some(ts);
                self.db
                    .set_cursor(&self.server.key, inner.cursor, inner.cursor_at)
                    .await?;
            }
        }
        Ok(())
    }

    /// Post already-fetched (or streamed) rows to the bridge channel.
    pub async fn deliver_rows(&self, rows: &[ChatRow]) -> eyre::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let settings = self.db.get_bridge_settings(&self.server.key).await?;
        if !settings.enabled || settings.channel_id.is_none() {
            return Ok(());
        }
        let channel_id = settings.channel_id.as_ref().unwrap();
        for payload in self.render(rows, &settings) {
            // Not `?` — a failure here must not block the cursor advance below.
            if let Err(err) = self.send_embeds(channel_id, payload).await {
                tracing::error!(
                    "[bridge:{}] failed to post to channel {channel_id}: {err:#}",
                    self.server.key
                );
                break;
            }
        }
        let mut inner = self.inner.write().await;
        if let Some(last) = rows.last() {
            if last.id > 0 {
                inner.cursor = last.id;
                inner.cursor_at = Some(last.received_at);
                self.db
                    .set_cursor(&self.server.key, inner.cursor, inner.cursor_at)
                    .await?;
            }
        }
        inner.last_post_at = Some(chrono::Utc::now());
        Ok(())
    }

    async fn deliver_feed(&self, settings: &crate::database::BridgeSettings) -> eyre::Result<()> {
        if !settings.enabled || settings.channel_id.is_none() {
            let (latest, at) = self.db.latest_chat_cursor(&self.server.key).await?;
            let (presence, presence_at) = self.db.latest_presence_cursor(&self.server.key).await?;
            let mut inner = self.inner.write().await;
            if latest > inner.cursor {
                inner.cursor = latest;
                inner.cursor_at = at;
                self.db.set_cursor(&self.server.key, latest, at).await?;
            }
            if presence > inner.presence_cursor {
                inner.presence_cursor = presence;
                inner.presence_cursor_at = presence_at;
                self.db
                    .set_presence_cursor(&self.server.key, presence, presence_at)
                    .await?;
            }
            return Ok(());
        }
        let (cursor, cursor_at, presence_cursor, presence_at) = {
            let inner = self.inner.read().await;
            (
                inner.cursor,
                inner.cursor_at,
                inner.presence_cursor,
                inner.presence_cursor_at,
            )
        };
        let kinds: Vec<String> = ROUTE_KINDS.iter().map(|k| (*k).to_string()).collect();
        let chat_rows = self
            .db
            .fetch_new_messages(
                &self.server.key,
                cursor,
                cursor_at,
                &kinds,
                self.config.bridge.max_rows_per_poll,
            )
            .await?;
        let presence_rows = self
            .db
            .fetch_new_presence_as_chat(
                &self.server.key,
                presence_cursor,
                presence_at,
                self.config.bridge.max_rows_per_poll,
            )
            .await?;
        if chat_rows.is_empty() && presence_rows.is_empty() {
            return Ok(());
        }

        let mut rows = chat_rows.clone();
        rows.extend(presence_rows.iter().cloned());
        rows.sort_by_key(|row| row.received_at);
        self.deliver_routed_rows(rows, settings).await?;

        let mut inner = self.inner.write().await;
        if let Some(last) = chat_rows.last() {
            inner.cursor = last.id;
            inner.cursor_at = Some(last.received_at);
            self.db
                .set_cursor(&self.server.key, inner.cursor, inner.cursor_at)
                .await?;
        }
        if let Some(last) = presence_rows.last() {
            inner.presence_cursor = last.id;
            inner.presence_cursor_at = Some(last.received_at);
            self.db
                .set_presence_cursor(
                    &self.server.key,
                    inner.presence_cursor,
                    inner.presence_cursor_at,
                )
                .await?;
        }
        Ok(())
    }

    async fn deliver_routed_rows(
        &self,
        rows: Vec<ChatRow>,
        settings: &crate::database::BridgeSettings,
    ) -> eyre::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let Some(fallback) = settings.channel_id.as_deref() else {
            return Ok(());
        };
        if !settings.enabled {
            return Ok(());
        }
        let n_in = rows.len();
        let rows = {
            let mut inner = self.inner.write().await;
            rows.into_iter()
                .filter_map(|row| Self::unique_presence(&mut inner.recent_presence, row))
                .collect::<Vec<_>>()
        };
        if rows.len() < n_in {
            tracing::info!(
                "[bridge:{}] dropped {} duplicate join/leave",
                self.server.key,
                n_in - rows.len()
            );
        }
        if rows.is_empty() {
            return Ok(());
        }
        let routes = self.db.get_event_routes(&self.server.key).await?;
        let mut by_channel: HashMap<String, Vec<ChatRow>> = HashMap::new();
        for row in &rows {
            if let Some(channel) = route_channel(row, fallback, &routes, &self.config.bridge.kinds)
            {
                by_channel.entry(channel).or_default().push(row.clone());
            }
        }
        let posted = !by_channel.is_empty();
        for (channel_id, channel_rows) in by_channel {
            for payload in self.render(&channel_rows, settings) {
                // Not `?` — a failure here must not block the cursor advance below.
                if let Err(err) = self.send_embeds(&channel_id, payload).await {
                    tracing::error!(
                        "[bridge:{}] failed to post to channel {channel_id}: {err:#}",
                        self.server.key
                    );
                    break;
                }
            }
        }
        if posted {
            self.inner.write().await.last_post_at = Some(chrono::Utc::now());
        }
        Ok(())
    }

    /// Each message, paired with whether it needs the fallback head attached.
    /// Head URLs come from cache only — see `heads.rs` for why this must not
    /// wait on the network.
    fn render(
        &self,
        rows: &[ChatRow],
        settings: &crate::database::BridgeSettings,
    ) -> Vec<Vec<serenity::builder::CreateEmbed>> {
        match settings.style {
            BridgeStyle::Rich => rows
                .chunks(self.config.bridge.embeds_per_message)
                .map(|chunk| {
                    let embeds = chunk
                        .iter()
                        .map(|row| {
                            let head = head_subject(row)
                                .and_then(|name| heads::url_for(name, &self.config));
                            build_event_embed(row, head.as_deref(), settings.rainbow)
                        })
                        .collect();
                    embeds
                })
                .collect(),
            BridgeStyle::Compact => pack_feed(rows, self.config.bridge.lines_per_message)
                .into_iter()
                .map(|batch| {
                    vec![build_feed_batch_embed(
                        &batch,
                        &self.server.label,
                        settings.rainbow,
                    )]
                })
                .collect(),
        }
    }

    /// Embeds a watched player's join/leave into the watchbridge channel.
    async fn deliver_watchbridge(&self, settings: &crate::database::BridgeSettings) -> eyre::Result<()> {
        let (cursor, cursor_at) = {
            let inner = self.inner.read().await;
            (inner.event_cursor, inner.event_cursor_at)
        };
        let Some(channel_id) = self.db.get_watchbridge_channel(&self.server.key).await? else {
            return self.advance_event_cursor(cursor).await;
        };

        let rows = self
            .db
            .fetch_watchbridge_hits(&self.server.key, cursor, cursor_at, WATCHBRIDGE_HITS_PER_TICK)
            .await?;

        if !rows.is_empty() {
            let deduped = {
                let mut inner = self.inner.write().await;
                rows.iter()
                    .cloned()
                    .filter_map(|row| Self::unique_presence(&mut inner.recent_watchbridge, row))
                    .collect::<Vec<_>>()
            };
            for payload in self.render(&deduped, settings) {
                if let Err(err) = self.send_embeds(&channel_id, payload).await {
                    tracing::error!(
                        "[bridge:{}] watchbridge post to {channel_id} failed: {err:#}",
                        self.server.key
                    );
                    break;
                }
            }
        }

        let (highest, highest_at) = if rows.len() as i64 == WATCHBRIDGE_HITS_PER_TICK {
            rows.last()
                .map(|r| (r.id, Some(r.received_at)))
                .unwrap_or((cursor, cursor_at))
        } else {
            self.db.latest_player_event_cursor(&self.server.key).await?
        };
        if highest > cursor {
            let mut inner = self.inner.write().await;
            inner.event_cursor = highest;
            inner.event_cursor_at = highest_at;
            self.db
                .set_event_cursor(&self.server.key, highest, highest_at)
                .await?;
        }
        Ok(())
    }

    /// No channel yet — still catch the cursor up to avoid a backlog dump later.
    async fn advance_event_cursor(&self, cursor: i64) -> eyre::Result<()> {
        let (latest, latest_at) = self.db.latest_player_event_cursor(&self.server.key).await?;
        if latest > cursor {
            let mut inner = self.inner.write().await;
            inner.event_cursor = latest;
            inner.event_cursor_at = latest_at;
            self.db
                .set_event_cursor(&self.server.key, latest, latest_at)
                .await?;
        }
        Ok(())
    }

    fn presence_dup_key(row: &ChatRow) -> Option<String> {
        let kind = crate::database::canonical_route_kind(&row.kind)?;
        if kind != "join" && kind != "leave" {
            return None;
        }
        let name = row
            .subject_name
            .as_deref()
            .or(row.sender_name.as_deref())?
            .trim()
            .to_ascii_lowercase();
        if name.is_empty() {
            return None;
        }
        let host = row.server_host.as_deref().unwrap_or("");
        Some(format!("{kind}:{host}:{name}"))
    }

    /// Drop a second join/leave for the same player within a few seconds
    /// (chat line + player_event, tab list, or two logger sessions).
    fn unique_presence(recent: &mut HashMap<String, Instant>, row: ChatRow) -> Option<ChatRow> {
        let Some(key) = Self::presence_dup_key(&row) else {
            return Some(row);
        };
        let now = Instant::now();
        recent.retain(|_, at| now.duration_since(*at) <= PRESENCE_DEDUP);
        if recent.contains_key(&key) {
            return None;
        }
        recent.insert(key, now);
        Some(row)
    }

    async fn send_embeds(
        &self,
        channel_id: &str,
        embeds: Vec<serenity::builder::CreateEmbed>,
    ) -> eyre::Result<()> {
        let id = ChannelId::new(channel_id.parse()?);
        let mut attempts = 0u8;
        loop {
            let msg = CreateMessage::new()
                .embeds(embeds.clone())
                .allowed_mentions(
                    serenity::builder::CreateAllowedMentions::new()
                        .all_users(false)
                        .all_roles(false)
                        .everyone(false),
                );
            match id.send_message(&self.http, msg).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    // Permanent errors (bad permissions, deleted channel) fail fast;
                    // anything else — including 429s and transient API hiccups like a
                    // non-JSON error body — gets a bounded, backed-off retry instead
                    // of silently losing the message on the first blip.
                    if is_permanent_discord_error(&err) {
                        return Err(err.into());
                    }
                    attempts += 1;
                    if attempts >= DISCORD_RETRY_ATTEMPTS {
                        return Err(err.into());
                    }
                    let wait = discord_429_wait(&err).unwrap_or_else(|| {
                        DISCORD_RETRY_FALLBACK.saturating_mul(u32::from(attempts))
                    });
                    let wait = wait.min(Duration::from_secs(15));
                    tracing::warn!(
                        "[bridge:{}] send to {channel_id} failed ({err}), retry {attempts}/{DISCORD_RETRY_ATTEMPTS} in {:.2}s",
                        self.server.key,
                        wait.as_secs_f32()
                    );
                    tokio::time::sleep(wait).await;
                }
            }
        }
    }
}

/// The player whose head belongs on this row.
fn head_subject(row: &ChatRow) -> Option<&str> {
    row.sender_name.as_deref().or(row.subject_name.as_deref())
}

fn accept_stream_event(
    last_seq: &mut u64,
    event: mc_stream::StreamEvent,
) -> Option<mc_stream::StreamEvent> {
    let seq = match &event {
        mc_stream::StreamEvent::Chat(c) => c.seq,
        mc_stream::StreamEvent::PlayerEvent(e) => e.seq,
    };
    if let Some(seq) = seq {
        if seq <= *last_seq {
            return None;
        }
        *last_seq = seq;
    }
    Some(event)
}

/// Errors no amount of retrying will fix — bad permissions or a channel
/// that's gone. Everything else (429s, transient API hiccups) gets retried.
fn is_permanent_discord_error(err: &serenity::Error) -> bool {
    let msg = err.to_string();
    msg.contains("Missing Access")
        || msg.contains("Missing Permissions")
        || msg.contains("Unknown Channel")
        || msg.contains("50013")
        || msg.contains("50001")
        || msg.contains("10003")
}

/// Retry-After from a real 429 body. Ignore Serenity's "line 1 column 1" decode text.
fn discord_429_wait(err: &serenity::Error) -> Option<Duration> {
    let serenity::Error::Http(serenity::http::HttpError::UnsuccessfulRequest(resp)) = err else {
        return None;
    };
    if resp.status_code.as_u16() != 429 {
        return None;
    }
    parse_retry_after_secs(&resp.error.message)
}

fn parse_retry_after_secs(msg: &str) -> Option<Duration> {
    if msg.contains("Could not decode json") {
        return None;
    }
    let lower = msg.to_ascii_lowercase();
    let rest = lower.find("retry").map(|i| &msg[i..]).unwrap_or(msg);
    rest.split(|c: char| !c.is_ascii_digit() && c != '.')
        .filter(|w| !w.is_empty() && w.contains('.'))
        .filter_map(|w| w.parse::<f64>().ok())
        .find(|n| *n > 0.0 && *n < 3600.0)
        .map(Duration::from_secs_f64)
}

pub struct BridgeSet {
    bridges: HashMap<String, Arc<Bridge>>,
}

impl BridgeSet {
    pub fn new(http: Arc<serenity::http::Http>, config: Arc<Config>, db: Arc<Db>) -> Self {
        let mut bridges = HashMap::new();
        for server in &config.servers {
            bridges.insert(
                server.key.clone(),
                Arc::new(Bridge::new(
                    http.clone(),
                    server.clone(),
                    config.clone(),
                    db.clone(),
                )),
            );
        }
        Self { bridges }
    }

    pub fn get(&self, key: &str) -> Option<Arc<Bridge>> {
        self.bridges.get(key).cloned()
    }

    /// The bot dropped off Discord and came back: tell every feed channel.
    pub async fn announce_bot_reconnect(&self) {
        for bridge in self.bridges.values() {
            bridge.announce_bot_reconnect().await;
        }
    }

    pub async fn start_all(self: &Arc<Self>) {
        let mut pending: Vec<Arc<Bridge>> = self.bridges.values().cloned().collect();

        async fn attempt(pending: Vec<Arc<Bridge>>) -> Vec<Arc<Bridge>> {
            let mut still = Vec::new();
            for bridge in pending {
                if let Err(err) = bridge.start().await {
                    tracing::error!("[bridge:{}] failed to start: {err}", bridge.key());
                    still.push(bridge);
                }
            }
            still
        }

        pending = attempt(pending).await;
        if pending.is_empty() {
            return;
        }
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(15));
            let mut pending = pending;
            loop {
                interval.tick().await;
                if pending.is_empty() {
                    break;
                }
                pending = attempt(pending).await;
            }
        });
    }

    pub fn stop_all(&self) {
        for bridge in self.bridges.values() {
            bridge.stop();
        }
    }
}
