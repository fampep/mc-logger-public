use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mc_stream::{
    encode_line, Frame, GatewayStatus, Hello, Ping, ProtocolError, Ready, Role, ServerStatus,
    StreamEvent, MIN_PROTOCOL_VERSION, PROTOCOL_VERSION,
};
use parking_lot::Mutex;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::time::timeout;

const BROADCAST_CAP: usize = 1024;
/// Warn about a producer's unreadable lines this often, so a broken logger
/// cannot flood the journal.
const MALFORMED_LOG_EVERY: u64 = 100;
/// Cap on one JSONL line — a chat event runs a few hundred bytes, so this
/// leaves generous headroom while still bounding memory. `tokio::io::Lines`
/// has no built-in cap: a client (authenticated or not — this applies to the
/// hello line too, read before the token check runs) that never sends `\n`
/// could otherwise force unbounded buffer growth.
const MAX_LINE_BYTES: usize = 64 * 1024;

/// Like `AsyncBufReadExt::read_line`, but refuses to grow the line past
/// `MAX_LINE_BYTES` instead of buffering an unterminated line forever.
async fn read_line_capped(reader: &mut BufReader<OwnedReadHalf>) -> std::io::Result<Option<String>> {
    let mut buf: Vec<u8> = Vec::new();
    loop {
        let chunk = reader.fill_buf().await?;
        if chunk.is_empty() {
            return Ok(if buf.is_empty() {
                None
            } else {
                Some(String::from_utf8_lossy(&buf).into_owned())
            });
        }
        if let Some(pos) = chunk.iter().position(|&b| b == b'\n') {
            buf.extend_from_slice(&chunk[..pos]);
            reader.consume(pos + 1);
            let line = String::from_utf8_lossy(&buf);
            return Ok(Some(line.strip_suffix('\r').unwrap_or(&line).to_string()));
        }
        if buf.len() + chunk.len() > MAX_LINE_BYTES {
            let consumed = chunk.len();
            reader.consume(consumed);
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("line exceeds {MAX_LINE_BYTES} bytes"),
            ));
        }
        buf.extend_from_slice(chunk);
        let consumed = chunk.len();
        reader.consume(consumed);
    }
}

pub const GATEWAY_VERSION: &str = concat!("terminal-client ", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// Events retained per server key for replay/resume.
    pub buffer_cap: usize,
    /// Shared secret every client must present, when set.
    pub token: Option<String>,
    /// When set, only these server keys are accepted.
    pub allowed_keys: Option<HashSet<String>>,
    /// How long a connection may take to send its hello line.
    pub hello_timeout: Duration,
    /// Drop a v2 producer that has not sent anything (not even a keepalive) for
    /// this long. v1 producers do not send keepalives and are never dropped.
    pub producer_idle: Duration,
    /// How often to ping an idle v2 consumer.
    pub ping_interval: Duration,
    /// How long a single write to a consumer may take before it is dropped.
    pub write_timeout: Duration,
    /// Producers + consumers allowed on one server key.
    pub max_conns_per_key: usize,
    /// Distinct server keys the gateway will hold at once.
    pub max_keys: usize,
    /// Drop an idle, unused server key's buffer after this long.
    pub evict_after: Duration,
    /// Lines one connection may relay into the game per minute. The logger
    /// paces its own sending, but nothing stopped a runaway client from filling
    /// that queue faster than it drains.
    pub say_per_min: u32,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            buffer_cap: 500,
            token: None,
            allowed_keys: None,
            hello_timeout: Duration::from_secs(10),
            producer_idle: Duration::from_secs(180),
            ping_interval: Duration::from_secs(30),
            write_timeout: Duration::from_secs(15),
            max_conns_per_key: 32,
            max_keys: 64,
            evict_after: Duration::from_secs(3600),
            say_per_min: 20,
        }
    }
}

/// One TCP listener that multiplexes live events by `Hello.server`.
pub struct Gateway {
    cfg: GatewayConfig,
    listen: Mutex<String>,
    started: Instant,
    connections: AtomicUsize,
    hubs: Mutex<HashMap<String, Arc<Hub>>>,
}

impl Gateway {
    pub fn new(cfg: GatewayConfig) -> Self {
        Self {
            cfg: GatewayConfig {
                buffer_cap: cfg.buffer_cap.max(1),
                ..cfg
            },
            listen: Mutex::new(String::new()),
            started: Instant::now(),
            connections: AtomicUsize::new(0),
            hubs: Mutex::new(HashMap::new()),
        }
    }

    pub fn config(&self) -> &GatewayConfig {
        &self.cfg
    }

    /// Existing hub for `server`, creating one if the key limit allows.
    fn hub_for(&self, server: &str) -> Option<Arc<Hub>> {
        let mut hubs = self.hubs.lock();
        if let Some(hub) = hubs.get(server) {
            return Some(Arc::clone(hub));
        }
        if hubs.len() >= self.cfg.max_keys {
            return None;
        }
        let hub = Arc::new(Hub::new(server.to_string(), self.cfg.buffer_cap));
        hubs.insert(server.to_string(), Arc::clone(&hub));
        Some(hub)
    }

    pub fn status(&self) -> GatewayStatus {
        let hubs: Vec<Arc<Hub>> = self.hubs.lock().values().cloned().collect();
        let mut servers: Vec<ServerStatus> = hubs
            .iter()
            .map(|hub| hub.status(self.cfg.allowed_keys.as_ref()))
            .collect();
        servers.sort_by(|a, b| a.server.cmp(&b.server));
        let mut allowed_keys: Vec<String> = self
            .cfg
            .allowed_keys
            .iter()
            .flatten()
            .cloned()
            .collect();
        allowed_keys.sort();
        GatewayStatus {
            allowed_keys,
            gateway: GATEWAY_VERSION.to_string(),
            listen: self.listen.lock().clone(),
            uptime_secs: self.started.elapsed().as_secs(),
            auth: self.cfg.token.is_some(),
            buffer_cap: self.cfg.buffer_cap,
            connections: self.connections.load(Ordering::Relaxed),
            servers,
        }
    }

    /// Recompute per-key event rates and drop server keys nobody is using.
    /// Called on a timer by `main`.
    pub fn maintain(&self) {
        let mut hubs = self.hubs.lock();
        hubs.retain(|key, hub| {
            hub.refresh_rate();
            let unused = hub.producers.load(Ordering::Relaxed) == 0
                && hub.consumers.load(Ordering::Relaxed) == 0
                && hub.idle_for() > self.cfg.evict_after;
            if unused {
                tracing::info!("[gateway] dropping idle server key \"{key}\" (no clients)");
            }
            !unused
        });
    }

    pub async fn serve(self: Arc<Self>, addr: SocketAddr) -> eyre::Result<()> {
        let listener = TcpListener::bind(addr).await.map_err(|err| {
            if err.kind() == std::io::ErrorKind::AddrInUse {
                eyre::eyre!(
                    "{addr} is already in use — another gateway is probably running \
                     (try: systemctl status terminal-client), or set LISTEN= to a free port"
                )
            } else {
                eyre::eyre!("cannot listen on {addr}: {err}")
            }
        })?;
        *self.listen.lock() = addr.to_string();

        loop {
            let (socket, peer) = match listener.accept().await {
                Ok(pair) => pair,
                // A failed accept (fd limit, reset during handshake) must not
                // take the gateway down with it.
                Err(err) => {
                    tracing::warn!("[gateway] accept failed: {err}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let gateway = Arc::clone(&self);
            tokio::spawn(async move {
                gateway.connections.fetch_add(1, Ordering::Relaxed);
                if let Err(err) = Arc::clone(&gateway).handle_client(socket, peer).await {
                    tracing::debug!("[gateway] {peer} closed: {err}");
                }
                gateway.connections.fetch_sub(1, Ordering::Relaxed);
            });
        }
    }

    async fn handle_client(self: Arc<Self>, socket: TcpStream, peer: SocketAddr) -> eyre::Result<()> {
        socket.set_nodelay(true)?;
        let (reader, mut writer) = socket.into_split();
        let mut lines = BufReader::new(reader);

        let hello_line = match timeout(self.cfg.hello_timeout, read_line_capped(&mut lines)).await {
            Ok(Ok(Some(line))) => line,
            Ok(Ok(None)) => eyre::bail!("{peer} disconnected before sending a hello"),
            Ok(Err(err)) => return Err(err.into()),
            Err(_) => {
                reject(
                    &mut writer,
                    ProtocolError::new(
                        "hello_timeout",
                        format!(
                            "no hello line within {}s",
                            self.cfg.hello_timeout.as_secs()
                        ),
                    ),
                )
                .await;
                eyre::bail!("{peer} sent no hello within the timeout");
            }
        };

        let hello: Hello = match serde_json::from_str(&hello_line) {
            Ok(hello) => hello,
            Err(err) => {
                reject(
                    &mut writer,
                    ProtocolError::new("malformed_hello", format!("cannot parse hello: {err}")),
                )
                .await;
                eyre::bail!("{peer} sent a malformed hello: {err}");
            }
        };

        let who = hello.client.clone().unwrap_or_else(|| "unknown".into());
        if hello.v < MIN_PROTOCOL_VERSION || hello.v > PROTOCOL_VERSION {
            reject(
                &mut writer,
                ProtocolError::new(
                    "bad_version",
                    format!(
                        "this gateway speaks protocol v{MIN_PROTOCOL_VERSION}-v{PROTOCOL_VERSION}, \
                         client sent v{}. Rebuild both sides from the same commit.",
                        hello.v
                    ),
                ),
            )
            .await;
            eyre::bail!("{peer} ({who}) speaks unsupported protocol v{}", hello.v);
        }

        if let Some(expected) = &self.cfg.token {
            if hello.token.as_deref() != Some(expected.as_str()) {
                reject(
                    &mut writer,
                    ProtocolError::new(
                        "unauthorized",
                        format!(
                            "this gateway requires a token; set {}= to the same value used here",
                            mc_stream::TOKEN_ENV
                        ),
                    ),
                )
                .await;
                tracing::warn!("[gateway] rejected {peer} ({who}): bad or missing token");
                eyre::bail!("unauthorized");
            }
        }

        if hello.role == Role::Status {
            let status = self.status();
            let _ = writer.write_all(&encode_line(&Frame::Status(status))?).await;
            let _ = writer.flush().await;
            return Ok(());
        }

        let server = hello.server.trim().to_string();
        if server.is_empty() {
            reject(
                &mut writer,
                ProtocolError::new(
                    "missing_server",
                    "hello has no server key — set SERVER_KEY= on the logger, or pass the key to mc-tail",
                ),
            )
            .await;
            eyre::bail!("{peer} ({who}) sent no server key");
        }

        if let Some(allowed) = &self.cfg.allowed_keys {
            if !allowed.contains(&server) {
                let mut known: Vec<&str> = allowed.iter().map(String::as_str).collect();
                known.sort_unstable();
                reject(
                    &mut writer,
                    ProtocolError::new(
                        "unknown_server",
                        format!(
                            "server key \"{server}\" is not in SERVER_KEYS. This gateway carries: {}",
                            known.join(", ")
                        ),
                    ),
                )
                .await;
                tracing::warn!(
                    "[gateway] rejected {peer} ({who}): server key \"{server}\" not in SERVER_KEYS"
                );
                eyre::bail!("unknown server key {server}");
            }
        }

        let Some(hub) = self.hub_for(&server) else {
            reject(
                &mut writer,
                ProtocolError::new(
                    "too_many_keys",
                    format!(
                        "gateway is already carrying {} server keys (MAX_KEYS)",
                        self.cfg.max_keys
                    ),
                ),
            )
            .await;
            eyre::bail!("key limit reached, refused {server}");
        };

        if hub.clients() >= self.cfg.max_conns_per_key {
            reject(
                &mut writer,
                ProtocolError::new(
                    "too_many_connections",
                    format!(
                        "server key \"{server}\" already has {} connections (MAX_CONNS_PER_KEY)",
                        hub.clients()
                    ),
                ),
            )
            .await;
            eyre::bail!("connection limit reached for {server}");
        }

        let ready = Frame::Ready(Ready {
            v: PROTOCOL_VERSION,
            role: hello.role,
            server: server.clone(),
            buffered: hub.buffered(),
            last_seq: hub.seq.load(Ordering::SeqCst),
            gateway: GATEWAY_VERSION.to_string(),
        });
        // v1 clients parse every inbound line as an event and would drop the
        // connection over a frame they do not know.
        let v2 = hello.v >= 2;
        if v2 {
            writer.write_all(&encode_line(&ready)?).await?;
            writer.flush().await?;
        }

        match hello.role {
            Role::Producer => {
                let idle = if v2 {
                    Some(self.cfg.producer_idle)
                } else {
                    None
                };
                hub.run_producer(lines, writer, peer, who, idle).await
            }
            Role::Consumer => {
                let ping = if v2 { Some(self.cfg.ping_interval) } else { None };
                hub.run_consumer(
                    lines,
                    writer,
                    ConsumerStart {
                        replay: hello.replay,
                        since_seq: hello.since_seq,
                        ping,
                        write_timeout: self.cfg.write_timeout,
                        say_per_min: self.cfg.say_per_min,
                    },
                    peer,
                    who,
                )
                .await
            }
            Role::Status => Ok(()),
        }
    }
}

async fn reject(writer: &mut OwnedWriteHalf, err: ProtocolError) {
    if let Ok(line) = encode_line(&Frame::Error(err)) {
        let _ = writer.write_all(&line).await;
        let _ = writer.flush().await;
    }
}

struct ConsumerStart {
    replay: u32,
    since_seq: Option<u64>,
    ping: Option<Duration>,
    write_timeout: Duration,
    say_per_min: u32,
}

/// Counts one connected client, and lists it, for as long as it lives — so the
/// status view stays correct however the connection ends.
struct ClientGuard {
    counter: Arc<AtomicUsize>,
    hub: Arc<Hub>,
    id: u64,
}

impl ClientGuard {
    fn new(hub: &Arc<Hub>, counter: &Arc<AtomicUsize>, info: mc_stream::ClientInfo) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        let id = hub.next_conn.fetch_add(1, Ordering::Relaxed);
        hub.conns.lock().insert(id, info);
        Self {
            counter: Arc::clone(counter),
            hub: Arc::clone(hub),
            id,
        }
    }
}

impl Drop for ClientGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
        self.hub.conns.lock().remove(&self.id);
    }
}

fn client_info(role: &str, who: &str, peer: SocketAddr, v: u32) -> mc_stream::ClientInfo {
    mc_stream::ClientInfo {
        role: role.to_string(),
        name: who.to_string(),
        peer: peer.to_string(),
        since_secs: 0,
        v,
    }
}

pub struct Hub {
    server: String,
    buffer_cap: usize,
    seq: AtomicU64,
    ring: Mutex<VecDeque<StreamEvent>>,
    tx: broadcast::Sender<StreamEvent>,
    /// Discord → logger. Producers subscribe; consumers publish.
    say_tx: broadcast::Sender<mc_stream::Say>,
    said: AtomicU64,
    producers: Arc<AtomicUsize>,
    consumers: Arc<AtomicUsize>,
    events_in: AtomicU64,
    events_out: AtomicU64,
    dropped: AtomicU64,
    /// Events the ring itself discarded because it filled up faster than they
    /// were consumed — distinct from `dropped`, which only counts a consumer
    /// falling behind the broadcast channel. Without this, the ring could
    /// silently lose events while `--status` still reported `dropped: 0`.
    ring_evicted: AtomicU64,
    malformed: AtomicU64,
    /// Unix millis of the last event, 0 when the key has never had one.
    last_event_ms: AtomicI64,
    /// Rolling events/minute, recomputed by `Gateway::maintain`.
    rate: Mutex<RateWindow>,
    /// Live connections on this key, for the status view.
    conns: Mutex<HashMap<u64, mc_stream::ClientInfo>>,
    next_conn: AtomicU64,
    created: Instant,
    last_activity: Mutex<Instant>,
}

struct RateWindow {
    at: Instant,
    events: u64,
    per_min: f64,
}

impl Hub {
    pub fn new(server: String, buffer_cap: usize) -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAP);
        let (say_tx, _) = broadcast::channel(64);
        Self {
            server,
            buffer_cap: buffer_cap.max(1),
            // Seeded from the clock so sequences keep rising across gateway
            // restarts: consumers dedupe by seq, and a counter that restarted
            // at 0 would make them discard every new event as "already seen".
            seq: AtomicU64::new(unix_millis() as u64),
            ring: Mutex::new(VecDeque::new()),
            tx,
            say_tx,
            said: AtomicU64::new(0),
            producers: Arc::new(AtomicUsize::new(0)),
            consumers: Arc::new(AtomicUsize::new(0)),
            events_in: AtomicU64::new(0),
            events_out: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            ring_evicted: AtomicU64::new(0),
            malformed: AtomicU64::new(0),
            last_event_ms: AtomicI64::new(0),
            rate: Mutex::new(RateWindow {
                at: Instant::now(),
                events: 0,
                per_min: 0.0,
            }),
            conns: Mutex::new(HashMap::new()),
            next_conn: AtomicU64::new(0),
            created: Instant::now(),
            last_activity: Mutex::new(Instant::now()),
        }
    }

    fn clients(&self) -> usize {
        self.producers.load(Ordering::Relaxed) + self.consumers.load(Ordering::Relaxed)
    }

    fn buffered(&self) -> usize {
        self.ring.lock().len()
    }

    fn idle_for(&self) -> Duration {
        self.last_activity.lock().elapsed()
    }

    fn touch(&self) {
        *self.last_activity.lock() = Instant::now();
    }

    fn refresh_rate(&self) {
        let total = self.events_in.load(Ordering::Relaxed);
        let mut rate = self.rate.lock();
        let mins = rate.at.elapsed().as_secs_f64() / 60.0;
        if mins >= 0.25 {
            rate.per_min = (total.saturating_sub(rate.events)) as f64 / mins;
            rate.at = Instant::now();
            rate.events = total;
        }
    }

    pub fn status(&self, allowed: Option<&HashSet<String>>) -> ServerStatus {
        let last_ms = self.last_event_ms.load(Ordering::Relaxed);
        let last_event_secs = if last_ms == 0 {
            None
        } else {
            Some(((unix_millis() - last_ms).max(0) / 1000) as u64)
        };
        let per_min = {
            let rate = self.rate.lock();
            if rate.per_min > 0.0 || self.created.elapsed() > Duration::from_secs(60) {
                rate.per_min
            } else {
                // Too young for a window; show the average so far rather than 0.
                let mins = self.created.elapsed().as_secs_f64() / 60.0;
                if mins > 0.0 {
                    self.events_in.load(Ordering::Relaxed) as f64 / mins
                } else {
                    0.0
                }
            }
        };
        ServerStatus {
            server: self.server.clone(),
            producers: self.producers.load(Ordering::Relaxed),
            consumers: self.consumers.load(Ordering::Relaxed),
            buffered: self.buffered(),
            events_in: self.events_in.load(Ordering::Relaxed),
            events_out: self.events_out.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            ring_evicted: self.ring_evicted.load(Ordering::Relaxed),
            malformed: self.malformed.load(Ordering::Relaxed),
            last_seq: self.seq.load(Ordering::Relaxed),
            last_event_secs,
            events_per_min: (per_min * 10.0).round() / 10.0,
            allowed: allowed.map_or(true, |set| set.contains(&self.server)),
            said: self.said.load(Ordering::Relaxed),
            clients: {
                let now = Instant::now();
                let mut list: Vec<mc_stream::ClientInfo> = self
                    .conns
                    .lock()
                    .values()
                    .cloned()
                    .collect();
                let _ = now;
                list.sort_by(|a, b| a.role.cmp(&b.role).then(a.name.cmp(&b.name)));
                list
            },
        }
    }

    /// Stamp, buffer and fan out one event from a producer.
    fn ingest(&self, mut event: StreamEvent) {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        event.set_seq(seq);
        {
            let mut ring = self.ring.lock();
            while ring.len() >= self.buffer_cap {
                ring.pop_front();
                self.ring_evicted.fetch_add(1, Ordering::Relaxed);
            }
            ring.push_back(event.clone());
        }
        self.events_in.fetch_add(1, Ordering::Relaxed);
        self.last_event_ms.store(unix_millis(), Ordering::Relaxed);
        self.touch();
        // Slow consumers miss; the lag handler below refills them from the ring.
        let _ = self.tx.send(event);
    }

    /// Buffered events after `since`, oldest first.
    fn replay_since(&self, since: u64) -> Vec<StreamEvent> {
        let ring = self.ring.lock();
        ring.iter()
            .filter(|e| e.seq().map_or(false, |s| s > since))
            .cloned()
            .collect()
    }

    /// The newest `n` buffered events, oldest first.
    fn replay_last(&self, n: usize) -> Vec<StreamEvent> {
        let ring = self.ring.lock();
        let start = ring.len().saturating_sub(n);
        ring.iter().skip(start).cloned().collect()
    }

    async fn run_producer(
        self: Arc<Self>,
        mut lines: BufReader<OwnedReadHalf>,
        mut writer: OwnedWriteHalf,
        peer: SocketAddr,
        who: String,
        idle: Option<Duration>,
    ) -> eyre::Result<()> {
        let _guard = ClientGuard::new(
            &self,
            &Arc::clone(&self.producers),
            client_info("producer", &who, peer, if idle.is_some() { 2 } else { 1 }),
        );
        tracing::info!(
            "[gateway] producer connected: {} from {peer} ({who})",
            self.server
        );
        self.touch();

        // Everything else on a producer connection flows towards us; `say` is
        // the one thing that flows back, so it gets its own writer task.
        let mut say_rx = self.say_tx.subscribe();
        let say_task = tokio::spawn(async move {
            loop {
                let say = match say_rx.recv().await {
                    Ok(say) => say,
                    // Lagging is not fatal: skip what was missed and keep
                    // listening. Exiting here left the logger deaf to Discord
                    // until it happened to reconnect.
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("[gateway] producer missed {n} say frame(s)");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                };
                let Ok(line) = encode_line(&Frame::Say(say)) else {
                    continue;
                };
                if writer.write_all(&line).await.is_err() || writer.flush().await.is_err() {
                    break;
                }
            }
        });

        loop {
            let next = match idle {
                Some(limit) => match timeout(limit, read_line_capped(&mut lines)).await {
                    Ok(res) => res?,
                    Err(_) => {
                        tracing::warn!(
                            "[gateway] producer {peer} ({}) went silent for {}s — dropping so it reconnects",
                            self.server,
                            limit.as_secs()
                        );
                        break;
                    }
                },
                None => read_line_capped(&mut lines).await?,
            };
            let Some(line) = next else { break };
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<Frame>(&line) {
                Ok(Frame::Chat(c)) => self.ingest(StreamEvent::Chat(c)),
                Ok(Frame::PlayerEvent(e)) => self.ingest(StreamEvent::PlayerEvent(e)),
                Ok(Frame::Ping(_)) => self.touch(),
                // One unreadable line used to kill the whole producer
                // connection, which reconnected and lost the rest of the batch.
                Ok(_) | Err(_) => {
                    let n = self.malformed.fetch_add(1, Ordering::Relaxed) + 1;
                    if n == 1 || n % MALFORMED_LOG_EVERY == 0 {
                        let preview: String = line.chars().take(160).collect();
                        tracing::warn!(
                            "[gateway] producer {peer} ({}) sent {n} unreadable line(s); skipping. Last: {preview}",
                            self.server
                        );
                    }
                }
            }
        }

        say_task.abort();
        tracing::info!(
            "[gateway] producer disconnected: {} from {peer} ({who})",
            self.server
        );
        Ok(())
    }

    /// Hand a Discord line to whichever loggers are producing for this key.
    fn relay_say(&self, say: mc_stream::Say, peer: SocketAddr) {
        if self.producers.load(Ordering::Relaxed) == 0 {
            tracing::warn!(
                "[gateway] dropping say for {}: no logger is connected",
                self.server
            );
            return;
        }
        self.said.fetch_add(1, Ordering::Relaxed);
        self.touch();
        tracing::info!(
            "[gateway] say → {} from {peer} ({}): {}",
            self.server,
            say.from.as_deref().unwrap_or("unknown"),
            say.text.chars().take(120).collect::<String>()
        );
        let _ = self.say_tx.send(say);
    }

    async fn run_consumer(
        self: Arc<Self>,
        mut lines: BufReader<OwnedReadHalf>,
        mut writer: OwnedWriteHalf,
        start: ConsumerStart,
        peer: SocketAddr,
        who: String,
    ) -> eyre::Result<()> {
        let _guard = ClientGuard::new(
            &self,
            &Arc::clone(&self.consumers),
            client_info("consumer", &who, peer, if start.ping.is_some() { 2 } else { 1 }),
        );
        let backfill = match start.since_seq {
            Some(since) => {
                tracing::info!(
                    "[gateway] consumer connected: {} from {peer} ({who}) resuming after seq {since}",
                    self.server
                );
                self.replay_since(since)
            }
            None => {
                tracing::info!(
                    "[gateway] consumer connected: {} from {peer} ({who}) replay={}",
                    self.server,
                    start.replay
                );
                self.replay_last(start.replay as usize)
            }
        };
        self.touch();

        // Consumers send `say` frames up this half; anything else is drained so a
        // chatty client cannot stall its own socket.
        {
            let hub = Arc::clone(&self);
            let peer_log = peer;
            let allowance = start.say_per_min;
            tokio::spawn(async move {
                let mut window = Instant::now();
                let mut used = 0u32;
                while let Ok(Some(line)) = read_line_capped(&mut lines).await {
                    if line.trim().is_empty() {
                        continue;
                    }
                    if let Ok(Frame::Say(say)) = serde_json::from_str::<Frame>(&line) {
                        if window.elapsed() >= Duration::from_secs(60) {
                            window = Instant::now();
                            used = 0;
                        }
                        used += 1;
                        if used > allowance {
                            // Log once per window rather than per dropped line.
                            if used == allowance + 1 {
                                tracing::warn!(
                                    "[gateway] {peer_log} exceeded {allowance} say/min for {};                                      dropping the rest of this minute",
                                    hub.server
                                );
                            }
                            continue;
                        }
                        hub.relay_say(say, peer_log);
                    }
                }
            });
        }

        // Subscribe before writing the backfill so events arriving mid-backfill
        // are queued rather than lost.
        let mut rx = self.tx.subscribe();
        let mut last_sent = start.since_seq.unwrap_or(0);
        for event in backfill {
            self.write_event(&mut writer, &event, start.write_timeout).await?;
            last_sent = event.seq().unwrap_or(last_sent).max(last_sent);
        }

        let ping_every = start.ping.unwrap_or(Duration::from_secs(3600));
        loop {
            tokio::select! {
                received = rx.recv() => match received {
                    Ok(event) => {
                        // Skip anything the backfill already delivered.
                        if event.seq().map_or(false, |s| s <= last_sent) {
                            continue;
                        }
                        self.write_event(&mut writer, &event, start.write_timeout).await?;
                        last_sent = event.seq().unwrap_or(last_sent).max(last_sent);
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // Refill from the ring instead of leaving a hole: the
                        // buffer usually still holds everything this consumer
                        // missed.
                        self.dropped.fetch_add(n, Ordering::Relaxed);
                        let missed = self.replay_since(last_sent);
                        let recovered = missed.len();
                        for event in missed {
                            self.write_event(&mut writer, &event, start.write_timeout).await?;
                            last_sent = event.seq().unwrap_or(last_sent).max(last_sent);
                        }
                        tracing::warn!(
                            "[gateway] consumer {peer} ({}) lagged by {n}; recovered {recovered} from the buffer",
                            self.server
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                _ = tokio::time::sleep(ping_every), if start.ping.is_some() => {
                    // A keepalive write is what turns a half-open socket into a
                    // disconnect instead of a feed that is silently dead.
                    let line = encode_line(&Frame::Ping(Ping::now()))?;
                    timeout(start.write_timeout, async {
                        writer.write_all(&line).await?;
                        writer.flush().await
                    })
                    .await
                    .map_err(|_| eyre::eyre!("consumer {peer} stopped reading"))??;
                }
            }
        }
        tracing::info!(
            "[gateway] consumer disconnected: {} from {peer} ({who})",
            self.server
        );
        Ok(())
    }

    async fn write_event(
        &self,
        writer: &mut OwnedWriteHalf,
        event: &StreamEvent,
        write_timeout: Duration,
    ) -> eyre::Result<()> {
        let line = encode_line(&Frame::from(event.clone()))?;
        timeout(write_timeout, async {
            writer.write_all(&line).await?;
            writer.flush().await
        })
        .await
        .map_err(|_| {
            eyre::eyre!(
                "consumer stopped reading for {}s; dropping it",
                write_timeout.as_secs()
            )
        })??;
        self.events_out.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use mc_stream::ChatEvent;

    fn hub() -> Arc<Hub> {
        Arc::new(Hub::new("test".into(), 3))
    }

    fn chat(text: &str) -> StreamEvent {
        StreamEvent::Chat(ChatEvent {
            seq: None,
            id: None,
            ts: Utc::now(),
            kind: "chat".into(),
            sender_name: Some("Notch".into()),
            sender_label: None,
            subject_name: None,
            killer_name: None,
            content: Some(text.into()),
            plain_text: text.into(),
            ansi: None,
            server_host: None,
        })
    }

    #[test]
    fn ring_keeps_only_the_newest_events() {
        let hub = hub();
        for text in ["a", "b", "c", "d"] {
            hub.ingest(chat(text));
        }
        let buffered = hub.replay_last(10);
        assert_eq!(buffered.len(), 3);
        assert_eq!(buffered[0].text(), "b");
        assert_eq!(buffered[2].text(), "d");
    }

    #[test]
    fn resume_returns_only_events_after_the_sequence() {
        let hub = hub();
        hub.ingest(chat("a"));
        hub.ingest(chat("b"));
        let first = hub.replay_last(10)[0].seq().unwrap();
        let after = hub.replay_since(first);
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].text(), "b");
    }

    #[test]
    fn sequences_survive_a_restart() {
        // Two hubs for the same key, as if the gateway had restarted between
        // them: the second must not hand out sequences the first already used,
        // or consumers would dedupe every new event away.
        let first = hub();
        first.ingest(chat("before restart"));
        let before = first.replay_last(1)[0].seq().unwrap();
        std::thread::sleep(Duration::from_millis(5));
        let second = hub();
        second.ingest(chat("after restart"));
        let after = second.replay_last(1)[0].seq().unwrap();
        assert!(after > before, "{after} should be greater than {before}");
    }

    #[test]
    fn key_limit_refuses_new_keys_but_keeps_serving_known_ones() {
        let gateway = Gateway::new(GatewayConfig {
            max_keys: 1,
            ..Default::default()
        });
        assert!(gateway.hub_for("one").is_some());
        assert!(gateway.hub_for("one").is_some(), "existing key still served");
        assert!(gateway.hub_for("two").is_none());
    }

    #[test]
    fn status_flags_keys_outside_the_allowlist() {
        let hub = hub();
        let mut allowed = HashSet::new();
        allowed.insert("other".to_string());
        assert!(!hub.status(Some(&allowed)).allowed);
        assert!(hub.status(None).allowed);
    }
}
