-- Schema for the azalea chat logger.
--
-- Improvements over the mineflayer version this replaces:
--   * sessions record the client and the ViaVersion target, so rows from
--     different clients/protocols stay comparable
--   * chat_messages keep the rendered ANSI alongside the plain text, so the
--     feed can be replayed exactly as it looked in the terminal
--   * player name changes are tracked instead of silently overwriting
--   * player_events carry the session's server, so cross-server queries do not
--     need a join

CREATE TABLE IF NOT EXISTS sessions (
  id             BIGSERIAL PRIMARY KEY,
  server_host    TEXT        NOT NULL,
  server_port    INTEGER     NOT NULL DEFAULT 25565,
  -- The protocol actually spoken to the server. With ViaVersion this differs
  -- from the version the client was built against, and that distinction is the
  -- whole reason this bot can reach servers the old one could not.
  target_version TEXT,
  client         TEXT        NOT NULL DEFAULT 'azalea',
  bot_username   TEXT,
  started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at       TIMESTAMPTZ,
  end_reason     TEXT
);

CREATE INDEX IF NOT EXISTS sessions_started_idx ON sessions (started_at DESC);
CREATE INDEX IF NOT EXISTS sessions_host_idx    ON sessions (server_host);

CREATE TABLE IF NOT EXISTS players (
  uuid       UUID PRIMARY KEY,
  name       TEXT        NOT NULL,
  first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS players_name_idx ON players (lower(name));

-- Every name a UUID has been seen under. The old schema overwrote the name in
-- place, which lost the history the moment somebody renamed.
CREATE TABLE IF NOT EXISTS player_names (
  uuid     UUID        NOT NULL,
  name     TEXT        NOT NULL,
  seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (uuid, name)
);

CREATE TABLE IF NOT EXISTS chat_messages (
  id            BIGSERIAL PRIMARY KEY,
  session_id    BIGINT REFERENCES sessions (id) ON DELETE CASCADE,
  received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- player | system | disguised: which packet carried it.
  source        TEXT        NOT NULL,
  -- chat | whisper | join | leave | death | advancement | server | unknown
  kind          TEXT        NOT NULL,
  sender_name   TEXT,
  sender_uuid   UUID,
  -- Who the line is *about*, for lines with no sender: the player who joined,
  -- left, or died. Consumers (the Discord bridge) need a name for these.
  subject_name  TEXT,
  -- Who did the killing, for deaths phrased "... by X". May be a mob rather
  -- than a player; join against `players` to count only PvP.
  killer_name   TEXT,
  content       TEXT,
  plain_text    TEXT        NOT NULL,
  -- Rendered with in-game colours, so the feed can be replayed verbatim.
  ansi          TEXT,
  translate_key TEXT,
  raw_json      JSONB
);

-- Applied separately so databases created before this column existed pick it up
-- without a reset (CREATE TABLE IF NOT EXISTS is a no-op on an existing table).
ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS subject_name TEXT;
ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS killer_name  TEXT;

CREATE INDEX IF NOT EXISTS chat_killer_idx  ON chat_messages (lower(killer_name));
CREATE INDEX IF NOT EXISTS chat_subject_idx ON chat_messages (lower(subject_name));

CREATE INDEX IF NOT EXISTS chat_session_idx  ON chat_messages (session_id);
CREATE INDEX IF NOT EXISTS chat_received_idx ON chat_messages (received_at DESC);
CREATE INDEX IF NOT EXISTS chat_sender_idx   ON chat_messages (lower(sender_name));
CREATE INDEX IF NOT EXISTS chat_kind_idx     ON chat_messages (kind);
-- Full-text search over the chat feed.
CREATE INDEX IF NOT EXISTS chat_text_idx
  ON chat_messages USING gin (to_tsvector('simple', plain_text));

CREATE TABLE IF NOT EXISTS player_events (
  id          BIGSERIAL PRIMARY KEY,
  session_id  BIGINT REFERENCES sessions (id) ON DELETE CASCADE,
  server_host TEXT,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- join | leave | snapshot
  event_type  TEXT        NOT NULL,
  -- tablist | chat: the tab list is authoritative, chat catches the rest.
  source      TEXT        NOT NULL,
  player_name TEXT        NOT NULL,
  player_uuid UUID,
  raw_text    TEXT
);

CREATE INDEX IF NOT EXISTS events_session_idx  ON player_events (session_id);
CREATE INDEX IF NOT EXISTS events_player_idx   ON player_events (lower(player_name));
CREATE INDEX IF NOT EXISTS events_occurred_idx ON player_events (occurred_at DESC);
CREATE INDEX IF NOT EXISTS events_type_idx     ON player_events (event_type);

-- Human-facing chat feed, newest first.
CREATE OR REPLACE VIEW recent_chat AS
  SELECT c.received_at, s.server_host, c.kind, c.sender_name, c.plain_text
  FROM chat_messages c
  LEFT JOIN sessions s ON s.id = c.session_id
  WHERE c.kind IN ('chat', 'whisper')
  ORDER BY c.received_at DESC;

-- Latest event per player per server, minus anyone whose latest event was a
-- leave, and only while the session that saw them is still connected.
--
-- That last condition is the whole point. A disconnect produces no RemovePlayer
-- for the people still in the tab list, so nothing writes them a 'leave' — and
-- without the join below they stayed "online" forever. It measured badly: 9b9t
-- reported 219 players online while the bot had been disconnected for an hour,
-- and 6b6t was 46% stale. Closing the session now retires its presence rows
-- implicitly, and reconnecting re-establishes the truth from the tab list
-- snapshot, so no synthetic leave rows are needed (inventing them would inflate
-- every leave count in the process).
--
-- Ties on occurred_at break by id: the writer batches, so two events for one
-- player can share a millisecond, and picking the wrong one flipped a leave back
-- to online.
CREATE OR REPLACE VIEW online_now AS
  SELECT latest.server_host, latest.player_name, latest.player_uuid, latest.occurred_at
  FROM (
    SELECT DISTINCT ON (server_host, lower(player_name))
           server_host, player_name, player_uuid, event_type, occurred_at, session_id
    FROM player_events
    ORDER BY server_host, lower(player_name), occurred_at DESC, id DESC
  ) latest
  JOIN sessions s ON s.id = latest.session_id AND s.ended_at IS NULL
  WHERE latest.event_type <> 'leave';

-- Player-versus-player kills only: both sides must be names the logger has
-- actually seen as players, which filters out mobs ("Zombie", "Creeper").
CREATE OR REPLACE VIEW player_kills AS
  SELECT c.received_at,
         c.killer_name  AS killer,
         c.subject_name AS victim,
         s.server_host,
         c.plain_text
  FROM chat_messages c
  LEFT JOIN sessions s ON s.id = c.session_id
  WHERE c.kind = 'death'
    AND c.killer_name IS NOT NULL
    AND c.subject_name IS NOT NULL
    AND EXISTS (SELECT 1 FROM players p WHERE lower(p.name) = lower(c.killer_name));

-- Kill/death tally per player, PvP only.
CREATE OR REPLACE VIEW kill_leaderboard AS
  SELECT name,
         sum(kills)  AS kills,
         sum(deaths) AS deaths
  FROM (
    SELECT killer AS name, 1 AS kills, 0 AS deaths FROM player_kills
    UNION ALL
    SELECT victim AS name, 0 AS kills, 1 AS deaths FROM player_kills
  ) tally
  GROUP BY name
  ORDER BY kills DESC, deaths ASC;

-- How long sessions actually last, per server. This is the number that showed
-- mineflayer capping out at ~35s on Constantiam.
CREATE OR REPLACE VIEW session_stats AS
  SELECT server_host,
         client,
         count(*)                                                          AS sessions,
         round(avg(EXTRACT(EPOCH FROM (ended_at - started_at)))::numeric, 1) AS avg_secs,
         round(max(EXTRACT(EPOCH FROM (ended_at - started_at)))::numeric, 1) AS max_secs
  FROM sessions
  WHERE ended_at IS NOT NULL
  GROUP BY server_host, client;

-- In-game plugin probe results (command tree / tab-complete / chat), one row
-- per scan. The Discord bot reads the newest row per database.
CREATE TABLE IF NOT EXISTS server_plugin_scans (
  id           BIGSERIAL PRIMARY KEY,
  session_id   BIGINT REFERENCES sessions (id) ON DELETE CASCADE,
  server_host  TEXT        NOT NULL,
  scanned_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  server_brand TEXT,
  methods      TEXT[]      NOT NULL DEFAULT '{}',
  plugins      JSONB       NOT NULL DEFAULT '[]',
  raw_notes    TEXT
);

CREATE INDEX IF NOT EXISTS plugin_scans_host_idx ON server_plugin_scans (server_host, scanned_at DESC);
