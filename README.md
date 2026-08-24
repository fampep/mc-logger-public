# mc-logger

A Minecraft chat logger and its Discord front end.

- **`azalea-bot/`** — Rust. Connects to a Minecraft server, classifies every
  chat line, and writes it to PostgreSQL. Optionally pushes the same events to
  the live event gateway.
- **`terminal-client/`** — Rust. One TCP JSONL gateway for every Minecraft
  server key. Logger produces; Discord and `mc-tail` consume.
- **`mc-stream/`** — Shared protocol crate used by the three above.
- **`mc-discord-bot/`** — Rust. Live feed via stream subscription (or DB poll
  fallback), plus slash commands that still read PostgreSQL.
- **`deploy/`** — provisioning script and systemd units for a Debian host.

## Why azalea and not mineflayer

mineflayer tops out at protocol 1.21.11. That is too old for some servers
(purityvanilla wants 26.1.2) and azalea alone is the opposite problem — it
speaks exactly one protocol per build, which made it *too new* for others
(Constantiam accepts at most 1.21.10, and rejected azalea's 26.1 outright).

`azalea-viaversion` resolves both. It runs ViaProxy underneath and translates,
so the server version becomes a runtime setting:

```
MC_TARGET_VERSION=1.21.10   # Constantiam
MC_TARGET_VERSION=26.1.2    # purityvanilla
```

One binary reaches every server. The switch also measurably improved session
length on Constantiam: mineflayer averaged 35s before being kicked, azalea
through ViaProxy has held for over an hour.

## Requirements

| Requirement | Why |
| --- | --- |
| **Rust nightly** | azalea depends on `simdnbt`, which uses `#![feature(portable_simd)]` |
| **Java 17+** | `azalea-viaversion` downloads and runs ViaProxy, a Java application |
| **PostgreSQL** | storage for both bots |
| **Rust stable** | Discord bot + terminal-client build on stable |

`rust-toolchain.toml` in `azalea-bot/` pins nightly for the logger. The Discord
bot and terminal-client are separate crates and build with the host toolchain.

## Live event streams (optional)

Without a stream, the Discord bot polls Postgres for new chat rows. With a
stream, the live path is push-based:

```
Minecraft → azalea-bot → terminal-client → mc-discord-bot
                    ↘
                     PostgreSQL (archive / slash commands)
```

One gateway process multiplexes every Minecraft server key on a single port.

1. Run the gateway:

```bash
cd terminal-client
cargo build --release
# or: systemctl enable --now terminal-client
LISTEN=127.0.0.1:9700 ./target/release/terminal-client
```

2. Point each logger at it (`azalea-bot/.env.*`):

```
EVENT_STREAM_ADDR=127.0.0.1:9700
SERVER_KEY=ninebninet
```

3. Point the Discord bot at the same gateway (`mc-discord-bot/.env`):

```
EVENT_STREAM_ADDR=127.0.0.1:9700
```

`SERVER_KEY` must match the Discord `SERVERS=` key. The Hello line is how the
gateway routes events; Discord opens one consumer per key.

Watch alerts and slash commands still use the database. If
`EVENT_STREAM_ADDR` / `SERVER_*_STREAM` is unset, the bridge keeps DB-polling —
and the feed channel says so on startup.

### Gateway settings

All optional; the defaults are what the unit file ships with.

| Variable | Default | What it does |
| --- | --- | --- |
| `LISTEN` | `127.0.0.1:9700` | Address, or just a port |
| `BUFFER` | `500` | Events kept per key for replay and resume |
| `SERVER_KEYS` | *(any)* | Allowlist. A typo'd `SERVER_KEY` is then rejected with the list of real ones instead of silently opening a feed nobody reads |
| `AUTH_TOKEN` | *(none)* | Shared secret; clients read it from `EVENT_STREAM_TOKEN` |
| `STATS_INTERVAL_SECS` | `300` | Health summary in the journal (`0` = off) |
| `PRODUCER_IDLE_SECS` | `180` | Drop a producer that stops sending keepalives |
| `SAY_PER_MIN` | `20` | Lines one client may relay into the game per minute |
| `MAX_CONNS_PER_KEY` / `MAX_KEYS` | `32` / `64` | Caps |

Protocol v2 adds keepalives, `since_seq` resume, and control frames, so a
rejected client is told *why* instead of watching the socket close. The gateway
still accepts v1 clients, which means it can be restarted before the loggers and
the Discord bot are rebuilt.

### mc-tail

```bash
mc-tail --status              # is the feed flowing? who is connected?
mc-tail ninebninet            # follow one server
mc-tail 2b2t -k death,join    # only these kinds
mc-tail 2b2t -p Herobrine     # only lines naming a player
mc-tail 2b2t --json | jq .    # raw stream
mc-tail 2b2t --say "brb"      # speak in game from the console
```

`--status` also lists every attached client by name, role and protocol version,
which is how you tell a second consumer or a logger still on v1 from a healthy
key — and it counts the lines relayed back into the game.

Lines come out looking like the game, because they *are* the game's: the logger
captures each chat component as Minecraft renders it — rank brackets, per-player
colours, formatting — and ships that down the stream alongside the plain text.
`mc-tail` prints the server's own rendering; `--badges` swaps back to the kind
column, and `--no-color` (or piping) strips the escapes. Tab-list joins and
leaves have no server line behind them, so those are drawn in vanilla's yellow.

With no server key it asks the gateway: one key means follow it, several means
list them. `--status` is the fastest answer to "why is Discord quiet?" — it
shows producers, consumers, events/min and the last event per key, and names the
likely cause (`no logger`, `no reader`, `quiet`). Full flags: `mc-tail --help`.
The old positional form (`mc-tail 127.0.0.1:9700 ninebninet 50`) still works.

## Standby loggers

A second Minecraft account can sit in-game beside the primary and take over
logging when the primary is kicked or its process dies. Both are connected the
whole time; only one writes.

```
LOGGER_ROLE=standby
LOGGER_ID=6b6t-backup          # unique; defaults to the env file's suffix
STANDBY_TAKEOVER_SECS=45
```

Everything else — `DATABASE_URL`, `MC_HOST`, `SERVER_KEY` — stays identical to
the primary's, so a takeover lands on the same feed. See
`azalea-bot/.env.standby.example`, then:

```bash
sudo systemctl enable --now azalea-bot-logger@.env.6b6t-backup
```

Every logger now upserts a row in `logger_heartbeats` (instance, role, session,
connected, writing). The standby writes only while no *primary* row for the same
host is both connected and fresh. Liveness has to be a heartbeat rather than
"did rows arrive recently", because on a quiet server a dead logger and a
healthy one look identical — and guessing wrong means either a gap in the log or
two loggers writing every line twice. The same rows now protect a live session
from `close_stale_sessions`.

An orderly disconnect hands over on the next heartbeat (~15s); a killed process
hands over after `STANDBY_TAKEOVER_SECS`. The standby's own sessions are stored
with `client = 'azalea-standby'` so two loggers do not read as one flapping
connection.

## Deploying

```bash
git clone git@github.com:USER/mc-logger.git
cd mc-logger
./deploy/setup.sh
```

The script installs everything, creates the database with a generated password
(written to `/etc/mc-logger-db-password`, mode 600), builds both bots, and
installs the systemd units. It is safe to re-run.

Then create the two `.env` files — **neither is in git** — and sign in to
Microsoft once interactively, because device-code auth cannot complete from a
systemd unit:

```bash
cd azalea-bot && ./target/release/azalea-bot   # approve the code, then Ctrl+C
sudo systemctl enable --now azalea-bot mc-discord-bot
journalctl -u azalea-bot -f
```

## Configuration

`azalea-bot/.env`:

```
DATABASE_URL=postgres://constantiam:PASSWORD@localhost:5432/constantiam
MC_EMAIL=you@example.com
MC_HOST=constantiam.net
MC_TARGET_VERSION=1.21.10
```

`mc-discord-bot/.env` — see `mc-discord-bot/.env.example`. The bridge channel is
normally set from Discord with `/chatbridge set` rather than configured here.

`RESET_DB=1` on the logger drops and recreates every table. It is off by
default and should stay that way.

## Discord commands

`/help` lists all of them in Discord. Every option that takes a player name
autocompletes from the database, and every paged view has buttons.

| Command | What it does |
| --- | --- |
| `/stats [player] [range]` | Player or server stats. Default window is the last 30 days (`7d` / `30d` / `90d` / `all`) |
| `/online` | Who the logger currently sees |
| `/events` | Compact 50-line log: chat, deaths, kills, joins, leaves, or advancements |
| `/leaderboard` | Rank by kills, K/D, deaths, messages, or joins |
| `/watch add\|remove\|list\|clear` | Get pinged when a player joins or leaves |
| `/server status` | Databases, feeds, backlog, uptime, logger sessions |
| `/server size` | Database size and stored totals |
| `/server plugins` | Plugins found by the in-game probe |
| `/server activity` | Hour-of-day and day-by-day histograms |
| `/chatbridge set\|status\|pause\|resume\|topic\|test\|customize` | Live chat feed (needs Manage Server) |
| `/queuebridge set\|status\|pause\|resume` | Live 2b2t queue embed |

The overview shows PvP kills, which are deaths where the killer is a name the
logger has also seen as a player — that filter is what keeps mobs off the
leaderboards.

### Stats and event lists

`/stats` opens on an overview with tabs. Event tabs render **50 rows in the
embed description** (not one field per event), with short relative timestamps
(`2m`, `14m`, `3h`). Pagination buttons page beyond the first 50.

Numbers are for the selected window. Lifetime totals are `/stats range:all`.
If a player has no activity in the window, the embed says so and shows when
they were last seen instead of a card of zeros.

### Feed connection notices

Every channel with `/chatbridge set` gets an embed when:

| Event | Embed |
| --- | --- |
| Bot starts | **Live feed connected** — streaming from `addr`, or **polling the database** when no gateway is configured |
| Event gateway drops | **Live feed disconnected** — retrying every 2s |
| Event gateway returns | **Live feed reconnected** — buffered events replayed |
| Bot reconnects to Discord | **Bot reconnected** — plus whether the feed survived the outage |

A dead gateway is otherwise indistinguishable from a quiet server. A gateway
loss is only announced after ~15s, so an ordinary reconnect posts nothing, and
a Discord *resume* (same session, no data lost) posts nothing either — only a
full reconnect does. Each post is logged as `posted <kind> notice to channel
<id>`. Turn them all off with `BRIDGE_STATUS_NOTICES=false`.

### The live feed

Rich style (the default) gives every line its own embed, laid out like a chat
client: the speaker bold at the start of the line, the player's head as the
small footer icon beside the timestamp, and one accent colour per kind down
the left edge. Chat uses the embed background colour, so its stripe disappears
and only deaths, joins and leaves catch the eye — unless rainbow mode is on,
which deliberately overrides that colour coding for every kind. Style and
rainbow are per-server settings stored in the database, live-switchable from
`/chatbridge customize` with no restart needed. `BRIDGE_STYLE=` in `.env` is
no longer read — every server starts on Rich until an admin changes it.

Not every name has a skin. Offline-mode servers are full of cracked accounts,
and the head service answers those with a 500, which Discord renders as a blank.
Names are checked against Mojang and the ones with no profile get a bundled
question-mark head (`mc-discord-bot/assets/unknown-head.png`), attached to the
message and referenced with `attachment://` — no hosting, no expiring CDN link.

The lookup happens **off** the delivery path. Resolving inline stalled the feed
outright: one lookup costs ~250ms and a busy anarchy server introduces dozens of
new names a minute. The render path reads a cache only, so the first line from
an unknown name may show a blank head and every line after it shows the question
mark.


`BRIDGE_STYLE=rich` (the default) gives every line its own embed with the
player's head. `BRIDGE_STYLE=compact` batches lines into one embed instead, so
the channel reads like the chat log it is — quieter on a busy server, at the cost
of the heads. Mirrored lines can never ping: only `/watch` alerts mention
anybody, and only the person who asked.

### Channel topics

Each feed channel's topic is rewritten with that server's own numbers, so the
header reads `2b2t.org · 528 online · peak 604 today · 1,506 players in 24h · 678
joins/h · 115 chat/h · 1,061 deaths in 24h · updated 07:49 UTC`. When the logger
is not connected it says so instead of claiming an empty server.

The bot needs **Manage Channel** in each feed channel. Discord allows only two
channel edits per ten minutes, so the refresh runs on a ten-minute timer
(`TOPIC_INTERVAL_MS`) and skips any edit that would not change the text;
`/chatbridge topic` forces one. Turn the whole thing off with
`TOPIC_UPDATES=false`.

### Watchlists

`/watch add player:<name>` stores a row in `discord_watchlist` in that server's
database, alongside the channel it was set up in. Alerts are driven by
`player_events` rather than chat, because the tab list is authoritative — a
server that never prints "X joined the game" is still covered. The same join
arriving from both sources within a minute only pings once.

## How classification works

Translation keys are used when the server sends them. Most servers flatten chat
to plain text instead, so there is a pattern fallback, including death matchers
built from all 105 vanilla `death.*` templates plus server-specific ones in
`custom_deaths.json`.

Templates are matched **longest first**. This is deliberate: `%1$s was squashed
by %2$s` also matches `"...squashed by a falling anvil while fighting Herobrine"`
and would credit the bystander with the kill. Sorting by length puts the precise
template first and makes matching deterministic — iterating the language
`HashMap` directly meant the same line could classify differently between runs.

To add a server's custom death message, add the template to
`azalea-bot/src/custom_deaths.json` (under the server key) and a case to the
tests. Templates phrased `... by %2$s` credit a killer; killer-first lines use
an object with `"victim"` / `"killer"` slot numbers.

## Migrating from another machine

On the old host:

```bash
pg_dump -U constantiam -d constantiam --no-owner --no-privileges -f mc-logger-dump.sql
```

Copy the dump and both `.env` files up, then:

```bash
./deploy/setup.sh      # once, installs everything and builds
./deploy/migrate.sh    # restores the dump and starts both services
```

`migrate.sh` refuses to overwrite a database that already has messages, applies
any schema added since the dump was taken, and enables both systemd units.

## Storage layout (r11)

A chat line is about 50 bytes of information. Stored naively in Postgres it
cost around 156 bytes on disk, measured over two million rows: roughly 36 bytes
of tuple header and line pointer, the player's name written out again on every
row, and a primary-key btree that only the Discord bridge's cursor ever read.

r8 addresses all three:

Measured over 2,000,000 chat rows and 300,000 events:

| | before | after | |
|---|---|---|---|
| chat heap | 124.8 B/row | 109.1 B/row | −13% |
| chat indexes | 31.5 B/row | 32.0 B/row | — |
| **chat total** | **156.2 B/row** | **141.0 B/row** | **−10%** |
| player_events | 132.8 B/row | 92.6 B/row | −30% |

The chat index barely moves: dropping the primary key freed ~31 B/row and the
new `(sender_id, received_at)` index costs about the same. That index is what
makes every per-player query in the Discord bot an index scan instead of a
sequential one, so it is worth its space — but it means the row-storage win on
chat is the heap alone.

* **`name_dict`** holds every name once — players, and mobs like `Wither
  Skeleton` that a death line can name. `chat_messages_raw` and
  `player_events_raw` reference it by `INT4`. `players` keeps its old meaning
  ("a real player we have seen") and gains a `name_id` link, so the PvP filters
  and the unique-player count are unchanged.
* **Column order is packed.** `8,8,4,4,4,4,1,1` leaves no alignment padding;
  an int4 placed between two 8-byte columns would have cost 4 bytes a row.
* **No primary key, no foreign key** on the log tables. The PK btree was ~31
  B/row for one cursor query, and an FK is enforced by a per-row trigger on
  every insert. The bridge cursor now reads the sequence instead.
* **`player_events` no longer repeats `player_uuid`.** It is stored only on the
  rare rows where it disagrees with the canonical `players.uuid` for that name.
* **Counters are rolled up, not triggered.** `stats_daily` and `player_daily`
  hold one row per day (and per player). The triggers they replaced updated a
  single counter row on every insert, which serialised every writer in the
  database behind one row lock and left a dead tuple each time.
* **Presence is state.** `player_presence` has one row per (session, player);
  `online_now` used to `DISTINCT ON` over the entire event log.

`chat_messages` and `player_events` still exist as views with the original
column names, so anything written against the old schema keeps working.

### Getting past 141 bytes a row

That is close to the floor for row storage: the message text is ~42 bytes of
it and Postgres' own per-row overhead is ~36 more. Compression is the only
lever left, and TOAST never fires on rows this small — the `lz4` setting the
old schema applied to `plain_text` was doing nothing.

Two options, neither of which changes the schema:

* **Filesystem compression.** Measured at **2.6x** on the real heap pages. Put
  `PGDATA` on ZFS with `compression=zstd`.
* **zstd archive for closed months.** `archive_old_partitions(keep_months)`
  packs each column of a closed month into one zstd frame. That drops the
  per-row tuple header entirely and lets compression see a whole column at
  once. Measured on a real month of 6b6t (386,479 rows, 51 MB live):

  | format | size | ratio |
  |---|---|---|
  | lz4 arrays | 7608 kB | 6.9x |
  | zstd, text column only | 5368 kB | 9.7x |
  | zstd + 64 KB trained dictionary | 5232 kB | 10.0x |
  | **zstd, every column** | **4944 kB** | **10.6x** |

  A trained dictionary is deliberately not used: it bought 2.5% over plain
  zstd, because batching already supplies the context a dictionary would.

  Per-message compression is a dead end and worth recording. On 30,000 real
  messages averaging 44 characters, compressing each one alone came out
  *larger* than the input (0.91x), and only 1.33x with a trained dictionary
  and every optional frame field stripped. The same messages batched compress
  6.8x. A 44-byte string has no context; its neighbours are the dictionary.

  ```bash
  ./deploy/install-pgzstd.sh sixbsixt twobtwot vanillaplus
  ./deploy/db-maintenance.sh --archive
  ```

## The hot window (r11)

r10 built all of the machinery above and then never fired it, because it only
packed **closed months**. A month is not closed until it ends, so on a database
that started this month the archive stayed empty and every row sat at its full
~138 bytes. The compression was real; nothing was ever fed to it.

r11 changes the unit from a month to a day, and packs everything older than a
hot window (`KEEP_DAYS`, default 7). Measured on one real day of 6b6t
(2026-08-16, 79,836 rows):

| | live rows | packed |
|---|---|---|
| bytes/row | ~138 B | **14.9 B** |
| text column alone | 3031 kB | 401 kB (7.6x) |

14.9 B/row is the whole cost including every index. Packing a 78 MB copy of
6b6t down to one hot day took it to **26 MB**, with all 395,032 chat rows and
153,236 events still readable and byte-identical through the views.

The window exists because the bridge cursor, `/events` and the channel topic
read the newest rows constantly, and decompressing a bucket to answer "what
happened in the last minute" would be absurd. Everything older is read rarely.

Three things follow from packing by day, and each is a correctness problem
rather than a nicety:

* **Rows leave a partition that is still being written**, so a day is `DELETE`d
  rather than the partition dropped — and only up to the highest id the pack
  actually saw. Bounding the delete by time instead would take out any row that
  landed between the pack and the delete.
* **Everything that reads history has to read both sides.** `chat_rows_between`
  / `chat_rows_for_name` (and the event equivalents) are the single definition
  of "the log between these two timestamps". This is not tidiness:
  `refresh_player_daily` used to read the physical tables alone, so re-running
  it over an archived day would have deleted that day's counters and rebuilt
  them from nothing.
* **Per-player queries must skip buckets without opening them**, so each bucket
  carries the sorted set of `name_dict` ids it mentions, GIN indexed. The median
  name appears in **1 bucket out of 23**; the busiest in 8.

### What this cost the Discord bot

Almost every number it shows now comes from `stats_daily` / `player_daily`
rather than from counting the log, which is both faster and archive-proof.
Playtime became a rollup column (`player_daily.playtime_secs`) — as a `lead()`
over the whole event log it was the one query that could not have survived
archiving at all. It agrees with the old computation to within per-spell integer
truncation: 69 seconds across 43.4 million.

Two deliberate consequences:

* `/stats range:7d` now means the last seven **UTC days**, not the last 168
  hours. Rollups are daily; the alternative was unpacking the archive on a
  command people run constantly.
* `/events` reads the hot window first and only falls through to the archive if
  that returns fewer rows than asked for — 9.5 ms against 1.16 s on the test
  database.

The one thing that is genuinely slower is unbounded text search: `/keyword` with
no player and no range has to open every bucket, by definition. Give it either a
player or a range and it prunes to a handful.

### Upgrading an existing database

```bash
./deploy/migrate-r11.sh
```

Refuses to start while any logger heartbeat is still live, takes a verified
`pg_dump` with a row-count manifest, applies r11, packs everything past the
window, rebuilds the rollups (`playtime_secs` is new, so every existing row has
a zero in it), `VACUUM FULL`s to actually return the space, and then compares
row counts through the compatibility views against the manifest — refusing to
finish if a single row is missing. Safe to re-run.

The loggers must be **rebuilt**, not just restarted: `schema.sql` is
`include_str!`'d into the binary and applied on every connect, so an old binary
would quietly reinstate the r10 rollup functions.

`./deploy/backup-dbs.sh` takes the same verified backup on its own.

r8 is still how a pre-dictionary database gets here:

```bash
./deploy/migrate-r8.sh
```

Stops the loggers, takes a verified `pg_dump` first, runs r7 if the tables are
still heaps, then r8, then compares row counts against the backup's manifest
and restarts. The migration commits one month at a time, so a large database
never needs a single enormous transaction, and it refuses to drop the
originals unless every row arrived.

`./deploy/db-maintenance.sh` is the periodic upkeep: partition creation, a
rollup catch-up for days a logger was down, and `--archive` to pack whatever has
aged out of the hot window. **Run it daily** — a day that is never packed just
stays at full size.
