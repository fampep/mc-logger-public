-- Server-seeker findings: Minecraft Java servers discovered via status ping.
-- Apply to the database pointed at by SEEKER_DATABASE_URL (or DATABASE_URL).

CREATE TABLE IF NOT EXISTS discovered_servers (
  id             BIGSERIAL PRIMARY KEY,
  ip             INET        NOT NULL,
  port           INTEGER     NOT NULL DEFAULT 25565,
  hostname       TEXT,
  motd_plain     TEXT        NOT NULL DEFAULT '',
  version_name   TEXT        NOT NULL DEFAULT '',
  protocol       INTEGER     NOT NULL DEFAULT 0,
  players_online INTEGER     NOT NULL DEFAULT 0,
  players_max    INTEGER     NOT NULL DEFAULT 0,
  matched_query  TEXT,
  first_seen     TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen      TIMESTAMPTZ NOT NULL DEFAULT now(),
  scan_source    TEXT        NOT NULL DEFAULT 'manual',
  raw_json       JSONB       NOT NULL DEFAULT '{}',
  UNIQUE (ip, port)
);

CREATE INDEX IF NOT EXISTS discovered_servers_last_seen_idx ON discovered_servers (last_seen DESC);
CREATE INDEX IF NOT EXISTS discovered_servers_query_idx ON discovered_servers (matched_query);
CREATE INDEX IF NOT EXISTS discovered_servers_motd_idx ON discovered_servers USING gin (to_tsvector('simple', motd_plain));

-- Discord bot bridge cursor (created by mc-discord-bot on startup).
CREATE TABLE IF NOT EXISTS discord_seeker_state (
  id              BOOLEAN PRIMARY KEY DEFAULT true,
  channel_id      TEXT,
  enabled         BOOLEAN     NOT NULL DEFAULT true,
  last_finding_id BIGINT      NOT NULL DEFAULT 0,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT discord_seeker_one_row CHECK (id)
);
