//! JSON-lines protocol for live Minecraft event streams.
//!
//! One terminal-client gateway multiplexes many server keys on a single TCP
//! port. Clients open a connection, send a hello line with `server`, then
//! either push events (producer), receive them (consumer), or ask for a
//! snapshot of the gateway (status).
//!
//! # Protocol v2
//!
//! v2 adds, all backwards compatible — the gateway still accepts v1 clients and
//! simply withholds the new frames from them:
//!
//! - **control frames** (`ready`, `error`, `ping`, `status`) share the event
//!   line channel, so a rejected client is told *why* instead of watching the
//!   socket close,
//! - **`since_seq`** resume, so a reconnecting consumer gets exactly the events
//!   it missed rather than a fixed-size replay window,
//! - **keepalives**, so a half-open TCP connection is noticed by both ends,
//! - **an optional shared token**, so a reachable port is not an open write
//!   channel into the feed.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

/// Protocol version this build speaks.
pub const PROTOCOL_VERSION: u32 = 2;
/// Oldest protocol version the gateway still accepts.
pub const MIN_PROTOCOL_VERSION: u32 = 1;

/// Environment variable holding the shared gateway token, if one is in use.
pub const TOKEN_ENV: &str = "EVENT_STREAM_TOKEN";

/// How often an idle producer sends a keepalive.
pub const KEEPALIVE: Duration = Duration::from_secs(30);
/// How long to wait before retrying a dropped connection.
const RECONNECT_DELAY: Duration = Duration::from_secs(2);
/// Longer backoff after the gateway rejects us outright — retrying a bad token
/// or an unknown server key every two seconds only fills the journal.
const REJECTED_DELAY: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Producer,
    Consumer,
    /// One-shot: the gateway answers with a [`GatewayStatus`] frame and closes.
    Status,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub v: u32,
    pub role: Role,
    /// Stable server key (matches Discord `SERVERS=` / logger instance).
    pub server: String,
    /// Consumers only: how many buffered events to replay on connect (0 = live only).
    #[serde(default)]
    pub replay: u32,
    /// Consumers only: replay everything buffered after this sequence number.
    /// Takes precedence over `replay`, and is how a reconnecting consumer picks
    /// up exactly where it left off.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since_seq: Option<u64>,
    /// Shared secret, when the gateway is configured with one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// Free-form client name, shown in gateway logs to make connections
    /// identifiable (`azalea-bot`, `mc-discord-bot`, `mc-tail`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client: Option<String>,
}

impl Hello {
    pub fn producer(server: impl Into<String>) -> Self {
        Self {
            v: PROTOCOL_VERSION,
            role: Role::Producer,
            server: server.into(),
            replay: 0,
            since_seq: None,
            token: token_from_env(),
            client: None,
        }
    }

    pub fn consumer(server: impl Into<String>, replay: u32) -> Self {
        Self {
            v: PROTOCOL_VERSION,
            role: Role::Consumer,
            server: server.into(),
            replay,
            since_seq: None,
            token: token_from_env(),
            client: None,
        }
    }

    pub fn status() -> Self {
        Self {
            v: PROTOCOL_VERSION,
            role: Role::Status,
            server: String::new(),
            replay: 0,
            since_seq: None,
            token: token_from_env(),
            client: None,
        }
    }

    pub fn with_token(mut self, token: Option<String>) -> Self {
        if token.is_some() {
            self.token = token;
        }
        self
    }

    pub fn with_client(mut self, client: impl Into<String>) -> Self {
        self.client = Some(client.into());
        self
    }

    pub fn with_since_seq(mut self, since_seq: Option<u64>) -> Self {
        self.since_seq = since_seq;
        self
    }
}

/// Reads the shared token from the environment, treating blank as unset.
pub fn token_from_env() -> Option<String> {
    std::env::var(TOKEN_ENV)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

// ---------------------------------------------------------------------------
// Wire frames
// ---------------------------------------------------------------------------

/// Everything that can appear on the wire. `chat` and `player_event` carry the
/// feed; the rest are control frames. Unknown types deserialize to
/// [`Frame::Unknown`] rather than failing, so a newer gateway can add frames
/// without knocking older clients off the stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Frame {
    Chat(ChatEvent),
    PlayerEvent(PlayerEvent),
    /// Gateway → client, once, after the hello is accepted.
    Ready(Ready),
    /// Gateway → client, immediately before the connection is closed.
    Error(ProtocolError),
    /// Either direction: proves the socket is still alive.
    Ping(Ping),
    /// Discord → gateway → logger: say this in game. Travels the opposite way
    /// to everything else on the connection.
    Say(Say),
    /// Gateway → status client.
    Status(GatewayStatus),
    #[serde(other)]
    Unknown,
}

impl Frame {
    /// The feed payload, if this frame carries one.
    pub fn into_event(self) -> Option<StreamEvent> {
        match self {
            Frame::Chat(c) => Some(StreamEvent::Chat(c)),
            Frame::PlayerEvent(e) => Some(StreamEvent::PlayerEvent(e)),
            _ => None,
        }
    }
}

impl From<StreamEvent> for Frame {
    fn from(event: StreamEvent) -> Self {
        match event {
            StreamEvent::Chat(c) => Frame::Chat(c),
            StreamEvent::PlayerEvent(e) => Frame::PlayerEvent(e),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ready {
    pub v: u32,
    pub role: Role,
    pub server: String,
    /// Events sitting in the server key's replay buffer.
    pub buffered: usize,
    /// Highest sequence the gateway has assigned for this key.
    pub last_seq: u64,
    /// Gateway build, for `mc-tail` and log lines.
    pub gateway: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolError {
    /// Machine-readable: `unauthorized`, `unknown_server`, `bad_version`,
    /// `missing_server`, `too_many_connections`, `malformed_hello`.
    pub code: String,
    pub message: String,
}

impl ProtocolError {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
        }
    }

    /// Rejections that will not fix themselves on an immediate retry.
    pub fn is_fatal(&self) -> bool {
        matches!(
            self.code.as_str(),
            "unauthorized" | "unknown_server" | "bad_version" | "missing_server"
        )
    }
}

/// A line to speak in game, sent from Discord.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Say {
    /// What to send. The logger decides whether it is allowed to send it.
    pub text: String,
    /// Who asked, for the logger's console and for auditing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
}

impl Say {
    pub fn new(text: impl Into<String>, from: Option<String>) -> Self {
        Self {
            text: text.into(),
            from,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Ping {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ts: Option<DateTime<Utc>>,
}

impl Ping {
    pub fn now() -> Self {
        Self {
            ts: Some(Utc::now()),
        }
    }
}

/// Snapshot of the whole gateway, answered to [`Role::Status`] clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayStatus {
    pub gateway: String,
    pub listen: String,
    pub uptime_secs: u64,
    /// Whether a shared token is required.
    pub auth: bool,
    pub buffer_cap: usize,
    pub connections: usize,
    /// Keys this gateway will accept. Empty means any key is accepted, so a
    /// client can tell "not connected yet" from "never going to work".
    #[serde(default)]
    pub allowed_keys: Vec<String>,
    pub servers: Vec<ServerStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerStatus {
    pub server: String,
    pub producers: usize,
    pub consumers: usize,
    /// Events currently held for replay.
    pub buffered: usize,
    pub events_in: u64,
    pub events_out: u64,
    /// Events a slow consumer missed (they resume via `since_seq`).
    pub dropped: u64,
    /// Events the ring buffer itself discarded because it filled up faster
    /// than they were read — distinct from `dropped`, which only counts a
    /// consumer falling behind the broadcast channel.
    #[serde(default)]
    pub ring_evicted: u64,
    /// Producer lines that failed to parse.
    pub malformed: u64,
    pub last_seq: u64,
    /// Seconds since the last event, or `None` if the key has never had one.
    pub last_event_secs: Option<u64>,
    pub events_per_min: f64,
    /// False when the key is not in the gateway's allowlist (allowlist off =
    /// every key is allowed).
    pub allowed: bool,
    /// Lines relayed from Discord into the game for this key.
    #[serde(default)]
    pub said: u64,
    /// Who is attached right now. Answers "why are there two consumers?"
    #[serde(default)]
    pub clients: Vec<ClientInfo>,
}

/// One live connection on a server key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientInfo {
    /// `producer` or `consumer`.
    pub role: String,
    /// Name the client gave in its hello (`azalea-bot`, `mc-discord-bot`, …).
    pub name: String,
    pub peer: String,
    pub since_secs: u64,
    /// Protocol version this connection speaks.
    pub v: u32,
}

impl ServerStatus {
    /// A one-word health summary, shared by `mc-tail --status` and the gateway's
    /// own periodic log line.
    pub fn health(&self) -> &'static str {
        if self.producers == 0 {
            "no logger"
        } else if self.consumers == 0 {
            "no reader"
        } else if self.last_event_secs.map_or(true, |s| s > 900) {
            "quiet"
        } else {
            "live"
        }
    }
}

// ---------------------------------------------------------------------------
// Feed payloads
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamEvent {
    Chat(ChatEvent),
    PlayerEvent(PlayerEvent),
}

impl StreamEvent {
    pub fn seq(&self) -> Option<u64> {
        match self {
            StreamEvent::Chat(c) => c.seq,
            StreamEvent::PlayerEvent(e) => e.seq,
        }
    }

    pub fn set_seq(&mut self, seq: u64) {
        match self {
            StreamEvent::Chat(c) => c.seq = Some(seq),
            StreamEvent::PlayerEvent(e) => e.seq = Some(seq),
        }
    }

    pub fn ts(&self) -> DateTime<Utc> {
        match self {
            StreamEvent::Chat(c) => c.ts,
            StreamEvent::PlayerEvent(e) => e.ts,
        }
    }

    /// `chat`, `death`, `join`, `leave`, … — the classifier's kind for chat
    /// rows, the event type for presence rows.
    pub fn kind(&self) -> &str {
        match self {
            StreamEvent::Chat(c) => &c.kind,
            StreamEvent::PlayerEvent(e) => &e.event_type,
        }
    }

    /// The player this line is about, when there is one.
    pub fn player(&self) -> Option<&str> {
        match self {
            StreamEvent::Chat(c) => c
                .sender_name
                .as_deref()
                .or(c.subject_name.as_deref())
                .or(c.sender_label.as_deref()),
            StreamEvent::PlayerEvent(e) => Some(&e.player_name),
        }
    }

    /// Everyone named by the line, for `mc-tail --player`.
    pub fn names(&self) -> Vec<&str> {
        match self {
            StreamEvent::Chat(c) => [
                c.sender_name.as_deref(),
                c.subject_name.as_deref(),
                c.killer_name.as_deref(),
                c.sender_label.as_deref(),
            ]
            .into_iter()
            .flatten()
            .collect(),
            StreamEvent::PlayerEvent(e) => vec![&e.player_name],
        }
    }

    pub fn text(&self) -> &str {
        match self {
            StreamEvent::Chat(c) => &c.plain_text,
            StreamEvent::PlayerEvent(e) => &e.event_type,
        }
    }

    /// The server's own rendering, when the logger captured one.
    pub fn ansi(&self) -> Option<&str> {
        match self {
            StreamEvent::Chat(c) => c.ansi.as_deref(),
            StreamEvent::PlayerEvent(e) => e.ansi.as_deref(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatEvent {
    /// Monotonic sequence assigned by the gateway (None from producer).
    #[serde(default)]
    pub seq: Option<u64>,
    /// Postgres id when known; live path usually leaves this unset.
    #[serde(default)]
    pub id: Option<i64>,
    pub ts: DateTime<Utc>,
    pub kind: String,
    #[serde(default)]
    pub sender_name: Option<String>,
    #[serde(default)]
    pub sender_label: Option<String>,
    #[serde(default)]
    pub subject_name: Option<String>,
    #[serde(default)]
    pub killer_name: Option<String>,
    #[serde(default)]
    pub content: Option<String>,
    pub plain_text: String,
    /// The line exactly as Minecraft drew it, ANSI escapes and all: rank
    /// colours, formatting, the lot. Consoles render this instead of the
    /// stripped text so a feed reads like the game. `None` from older loggers
    /// and for lines nobody ever printed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ansi: Option<String>,
    #[serde(default)]
    pub server_host: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerEvent {
    #[serde(default)]
    pub seq: Option<u64>,
    pub ts: DateTime<Utc>,
    pub event_type: String,
    pub source: String,
    pub player_name: String,
    /// Set when the event came from a chat line the server drew itself; tab-list
    /// presence has no text behind it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ansi: Option<String>,
    #[serde(default)]
    pub server_host: Option<String>,
}

pub fn encode_line<T: Serialize>(value: &T) -> eyre::Result<Vec<u8>> {
    let mut line = serde_json::to_vec(value)?;
    line.push(b'\n');
    Ok(line)
}

pub fn decode_line<T: for<'de> Deserialize<'de>>(line: &[u8]) -> eyre::Result<T> {
    Ok(serde_json::from_slice(line)?)
}

// ---------------------------------------------------------------------------
// Producer
// ---------------------------------------------------------------------------

/// Fire-and-forget producer that reconnects forever.
#[derive(Clone)]
pub struct StreamPublisher {
    addr: String,
    server: String,
    connected: Arc<AtomicBool>,
    tx: tokio::sync::mpsc::UnboundedSender<StreamEvent>,
}

impl StreamPublisher {
    pub fn spawn(addr: String, server: String) -> Self {
        Self::spawn_with(addr, server, token_from_env())
    }

    /// Same, but hands back the channel the gateway's `say` frames arrive on —
    /// the reverse path, Discord to the game.
    pub fn spawn_with_says(
        addr: String,
        server: String,
        token: Option<String>,
    ) -> (Self, tokio::sync::mpsc::UnboundedReceiver<Say>) {
        let (say_tx, say_rx) = tokio::sync::mpsc::unbounded_channel();
        (
            Self::build(addr, server, token, Some(say_tx)),
            say_rx,
        )
    }

    pub fn spawn_with(addr: String, server: String, token: Option<String>) -> Self {
        Self::build(addr, server, token, None)
    }

    fn build(
        addr: String,
        server: String,
        token: Option<String>,
        says: Option<tokio::sync::mpsc::UnboundedSender<Say>>,
    ) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<StreamEvent>();
        let connected = Arc::new(AtomicBool::new(false));
        let addr_task = addr.clone();
        let server_task = server.clone();
        let flag = Arc::clone(&connected);
        tokio::spawn(async move {
            loop {
                let outcome = run_producer(
                    &addr_task,
                    &server_task,
                    token.clone(),
                    &flag,
                    &mut rx,
                    says.clone(),
                )
                .await;
                flag.store(false, Ordering::Relaxed);
                let delay = match outcome {
                    Ok(Some(err)) => {
                        tracing::error!(
                            "[stream:{server_task}] gateway rejected producer: {} ({}); retrying in {}s",
                            err.message,
                            err.code,
                            REJECTED_DELAY.as_secs()
                        );
                        REJECTED_DELAY
                    }
                    Ok(None) => {
                        tracing::warn!(
                            "[stream:{server_task}] producer ended; reconnecting in {}s",
                            RECONNECT_DELAY.as_secs()
                        );
                        RECONNECT_DELAY
                    }
                    Err(err) => {
                        tracing::warn!(
                            "[stream:{server_task}] producer error: {err}; reconnecting in {}s",
                            RECONNECT_DELAY.as_secs()
                        );
                        RECONNECT_DELAY
                    }
                };
                sleep(delay).await;
            }
        });
        Self {
            addr,
            server,
            connected,
            tx,
        }
    }

    pub fn publish(&self, event: StreamEvent) {
        let _ = self.tx.send(event);
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn server(&self) -> &str {
        &self.server
    }

    /// Whether the gateway connection is currently up.
    pub fn connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }
}

/// `Ok(Some(err))` means the gateway rejected us and a fast retry is pointless.
async fn run_producer(
    addr: &str,
    server: &str,
    token: Option<String>,
    connected: &AtomicBool,
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<StreamEvent>,
    says: Option<tokio::sync::mpsc::UnboundedSender<Say>>,
) -> eyre::Result<Option<ProtocolError>> {
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    let (reader, mut writer) = stream.into_split();
    let (err_tx, mut err_rx) = tokio::sync::mpsc::unbounded_channel::<ProtocolError>();

    // The gateway talks back now: `ready` on accept, `error` on rejection.
    let server_log = server.to_string();
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            match serde_json::from_str::<Frame>(&line) {
                Ok(Frame::Ready(ready)) => tracing::info!(
                    "[stream:{server_log}] gateway {} accepted producer (buffered={})",
                    ready.gateway,
                    ready.buffered
                ),
                Ok(Frame::Error(err)) => {
                    let _ = err_tx.send(err);
                    break;
                }
                // Someone in Discord wants this said in game.
                Ok(Frame::Say(say)) => {
                    if let Some(says) = &says {
                        let _ = says.send(say);
                    }
                }
                _ => {}
            }
        }
    });

    let hello = Hello::producer(server)
        .with_token(token)
        .with_client("azalea-bot");
    writer.write_all(&encode_line(&hello)?).await?;
    writer.flush().await?;
    connected.store(true, Ordering::Relaxed);
    tracing::info!("[stream:{server}] producer connected to {addr}");

    loop {
        tokio::select! {
            biased;
            Some(err) = err_rx.recv() => return Ok(Some(err)),
            event = rx.recv() => {
                let Some(event) = event else { return Ok(None) };
                writer.write_all(&encode_line(&Frame::from(event))?).await?;
                writer.flush().await?;
            }
            // A silent Minecraft server is normal; a silent socket is not.
            // The keepalive is what turns a half-open connection into an error.
            _ = sleep(KEEPALIVE) => {
                writer.write_all(&encode_line(&Frame::Ping(Ping::now()))?).await?;
                writer.flush().await?;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Consumer
// ---------------------------------------------------------------------------

/// Options for [`StreamSubscriber`].
#[derive(Debug, Clone)]
pub struct SubscribeOptions {
    /// Events to replay on the *first* connection.
    pub replay: u32,
    /// Start after this sequence instead of replaying a fixed window.
    pub since: Option<u64>,
    /// Token override; defaults to `EVENT_STREAM_TOKEN`.
    pub token: Option<String>,
    /// Name shown in gateway logs.
    pub client: Option<String>,
}

impl Default for SubscribeOptions {
    fn default() -> Self {
        Self {
            replay: 0,
            since: None,
            token: token_from_env(),
            client: None,
        }
    }
}

/// Consumer that reconnects and yields events on a channel.
///
/// After the first connection it resumes with `since_seq`, so a reconnect
/// delivers exactly the events that were missed — no gap, no replay flood.
pub struct StreamSubscriber {
    pub rx: tokio::sync::mpsc::UnboundedReceiver<StreamEvent>,
    last_seq: Arc<AtomicU64>,
    connected: Arc<AtomicBool>,
    /// Outbound `say` frames, picked up by whichever connection is live.
    say_tx: tokio::sync::mpsc::UnboundedSender<Say>,
}

impl StreamSubscriber {
    pub fn spawn(addr: String, server: String, replay: u32) -> Self {
        Self::spawn_with(
            addr,
            server,
            SubscribeOptions {
                replay,
                ..Default::default()
            },
        )
    }

    pub fn spawn_with(addr: String, server: String, opts: SubscribeOptions) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (say_tx, say_rx) = tokio::sync::mpsc::unbounded_channel::<Say>();
        let say_rx = Arc::new(tokio::sync::Mutex::new(say_rx));
        // A caller-supplied resume point behaves exactly like one we learned
        // ourselves: connect asking for everything after it.
        let last_seq = Arc::new(AtomicU64::new(opts.since.unwrap_or(0)));
        let connected = Arc::new(AtomicBool::new(false));
        let seq_task = Arc::clone(&last_seq);
        let flag = Arc::clone(&connected);
        let say_rx_task = Arc::clone(&say_rx);
        tokio::spawn(async move {
            loop {
                let outcome = run_consumer(
                    &addr,
                    &server,
                    &opts,
                    &seq_task,
                    &flag,
                    &tx,
                    &say_rx_task,
                )
                .await;
                flag.store(false, Ordering::Relaxed);
                let delay = match outcome {
                    Ok(Some(err)) => {
                        tracing::error!(
                            "[stream:{server}] gateway rejected consumer: {} ({}); retrying in {}s",
                            err.message,
                            err.code,
                            REJECTED_DELAY.as_secs()
                        );
                        REJECTED_DELAY
                    }
                    Ok(None) => {
                        tracing::warn!(
                            "[stream:{server}] consumer ended; reconnecting in {}s",
                            RECONNECT_DELAY.as_secs()
                        );
                        RECONNECT_DELAY
                    }
                    Err(err) => {
                        tracing::warn!(
                            "[stream:{server}] consumer error: {err}; reconnecting in {}s",
                            RECONNECT_DELAY.as_secs()
                        );
                        RECONNECT_DELAY
                    }
                };
                if tx.is_closed() {
                    break;
                }
                sleep(delay).await;
            }
        });
        Self {
            rx,
            last_seq,
            connected,
            say_tx,
        }
    }

    /// Ask the logger on the other end to speak this line in game. Queued if
    /// the connection is down, and sent when it comes back.
    pub fn say(&self, text: impl Into<String>, from: Option<String>) {
        let _ = self.say_tx.send(Say::new(text, from));
    }

    /// A cloneable sender, for callers that keep the subscriber's receiver
    /// somewhere else (the Discord bridge moves `rx` into its own task).
    pub fn say_handle(&self) -> SayHandle {
        SayHandle {
            tx: self.say_tx.clone(),
        }
    }

    /// Highest sequence handed to the caller so far.
    pub fn last_seq(&self) -> u64 {
        self.last_seq.load(Ordering::Relaxed)
    }

    pub fn connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    /// Shared handle to the connection state, for callers that want to react to
    /// the link going up and down (the Discord bridge announces both).
    pub fn connection_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.connected)
    }
}

#[allow(clippy::too_many_arguments)]
/// Detached handle for sending lines back to the logger.
#[derive(Clone)]
pub struct SayHandle {
    tx: tokio::sync::mpsc::UnboundedSender<Say>,
}

impl SayHandle {
    pub fn say(&self, text: impl Into<String>, from: Option<String>) -> bool {
        self.tx.send(Say::new(text, from)).is_ok()
    }
}

async fn run_consumer(
    addr: &str,
    server: &str,
    opts: &SubscribeOptions,
    last_seq: &AtomicU64,
    connected: &AtomicBool,
    tx: &tokio::sync::mpsc::UnboundedSender<StreamEvent>,
    says: &Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Say>>>,
) -> eyre::Result<Option<ProtocolError>> {
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    let (reader, mut writer) = stream.into_split();

    let seen = last_seq.load(Ordering::Relaxed);
    let mut hello = Hello::consumer(server, opts.replay)
        .with_token(opts.token.clone())
        .with_since_seq(if seen > 0 { Some(seen) } else { None });
    if let Some(client) = &opts.client {
        hello = hello.with_client(client.clone());
    }
    writer.write_all(&encode_line(&hello)?).await?;
    writer.flush().await?;

    let mut lines = BufReader::new(reader).lines();
    let mut says = says.lock().await;
    // Two directions on one socket now: events coming down, `say` going up.
    // The gateway's pings are what surface a dead link on a quiet server.
    loop {
        let line = tokio::select! {
            line = lines.next_line() => match line? {
                Some(line) => line,
                None => break,
            },
            say = says.recv() => {
                let Some(say) = say else { break };
                writer.write_all(&encode_line(&Frame::Say(say))?).await?;
                writer.flush().await?;
                continue;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let frame: Frame = match serde_json::from_str(&line) {
            Ok(frame) => frame,
            // One bad line must not tear down a working feed.
            Err(err) => {
                tracing::warn!("[stream:{server}] skipping unreadable line: {err}");
                continue;
            }
        };
        match frame {
            Frame::Chat(_) | Frame::PlayerEvent(_) => {
                // Events arriving is proof of a live link even from a gateway
                // too old to send `ready`.
                connected.store(true, Ordering::Relaxed);
                let event = frame.into_event().expect("event frame");
                if let Some(seq) = event.seq() {
                    last_seq.fetch_max(seq, Ordering::Relaxed);
                }
                if tx.send(event).is_err() {
                    return Ok(None);
                }
            }
            Frame::Ready(ready) => {
                connected.store(true, Ordering::Relaxed);
                // A gateway whose sequences are behind ours is a different (or
                // rebuilt) gateway; keeping the old resume point would make us
                // discard everything it sends as already seen.
                if ready.last_seq > 0 && ready.last_seq < seen {
                    tracing::warn!(
                        "[stream:{server}] gateway sequences restarted (theirs {}, ours {seen}); \
                         taking the feed from the top",
                        ready.last_seq
                    );
                    last_seq.store(0, Ordering::Relaxed);
                }
                let resume = if seen > 0 {
                    format!("resuming after seq {seen}")
                } else {
                    format!("replay={}", opts.replay)
                };
                tracing::info!(
                    "[stream:{server}] consumer connected to {addr} ({resume}, gateway {})",
                    ready.gateway
                );
            }
            Frame::Error(err) => return Ok(Some(err)),
            // `say` only travels the other way; a gateway echoing one back has
            // nothing for us to do.
            Frame::Ping(_) | Frame::Status(_) | Frame::Say(_) | Frame::Unknown => {}
        }
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Ask a gateway for a snapshot of every server key it is carrying.
pub async fn fetch_status(addr: &str, token: Option<String>) -> eyre::Result<GatewayStatus> {
    let connect = timeout(Duration::from_secs(5), TcpStream::connect(addr))
        .await
        .map_err(|_| eyre::eyre!("timed out connecting to {addr}"))??;
    connect.set_nodelay(true)?;
    let (reader, mut writer) = connect.into_split();
    let hello = Hello::status()
        .with_token(token)
        .with_client("mc-tail");
    writer.write_all(&encode_line(&hello)?).await?;
    writer.flush().await?;

    let mut lines = BufReader::new(reader).lines();
    let line = timeout(Duration::from_secs(5), lines.next_line())
        .await
        .map_err(|_| eyre::eyre!("timed out waiting for status from {addr}"))??
        .ok_or_else(|| {
            eyre::eyre!("{addr} closed without answering — is it an older gateway (pre-v2)?")
        })?;

    match serde_json::from_str::<Frame>(&line)? {
        Frame::Status(status) => Ok(status),
        Frame::Error(err) => {
            eyre::bail!("{} ({})", err.message, err.code)
        }
        _ => {
            eyre::bail!("unexpected reply from {addr}: {line}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chat(seq: u64) -> StreamEvent {
        StreamEvent::Chat(ChatEvent {
            seq: Some(seq),
            id: None,
            ts: Utc::now(),
            kind: "chat".into(),
            sender_name: Some("Notch".into()),
            sender_label: None,
            subject_name: None,
            killer_name: None,
            content: Some("hi".into()),
            plain_text: "<Notch> hi".into(),
            ansi: None,
            server_host: None,
        })
    }

    #[test]
    fn event_frames_keep_their_v1_wire_tags() {
        let line = String::from_utf8(encode_line(&Frame::from(chat(7))).unwrap()).unwrap();
        assert!(line.contains(r#""type":"chat""#));
        // A v1 peer decodes the same bytes as a StreamEvent.
        let event: StreamEvent = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(event.seq(), Some(7));
    }

    #[test]
    fn unknown_frame_types_are_ignored_not_fatal() {
        let frame: Frame = serde_json::from_str(r#"{"type":"future_thing","x":1}"#).unwrap();
        assert!(matches!(frame, Frame::Unknown));
    }

    #[test]
    fn v1_hello_still_parses() {
        let hello: Hello =
            serde_json::from_str(r#"{"v":1,"role":"consumer","server":"ninebninet","replay":50}"#)
                .unwrap();
        assert_eq!(hello.role, Role::Consumer);
        assert_eq!(hello.replay, 50);
        assert!(hello.since_seq.is_none());
        assert!(hello.token.is_none());
    }

    #[test]
    fn names_covers_killer_for_player_filters() {
        let event = StreamEvent::Chat(ChatEvent {
            killer_name: Some("Herobrine".into()),
            ..match chat(1) {
                StreamEvent::Chat(c) => c,
                _ => unreachable!(),
            }
        });
        assert!(event.names().contains(&"Herobrine"));
        assert!(event.names().contains(&"Notch"));
    }

    #[test]
    fn health_reads_the_common_failure_first() {
        let mut s = ServerStatus {
            said: 0,
            clients: Vec::new(),
            server: "ninebninet".into(),
            producers: 0,
            consumers: 1,
            buffered: 0,
            events_in: 0,
            events_out: 0,
            dropped: 0,
            ring_evicted: 0,
            malformed: 0,
            last_seq: 0,
            last_event_secs: Some(3),
            events_per_min: 0.0,
            allowed: true,
        };
        assert_eq!(s.health(), "no logger");
        s.producers = 1;
        s.consumers = 0;
        assert_eq!(s.health(), "no reader");
        s.consumers = 1;
        assert_eq!(s.health(), "live");
        s.last_event_secs = Some(4_000);
        assert_eq!(s.health(), "quiet");
    }
}
