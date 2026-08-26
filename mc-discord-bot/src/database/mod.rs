//! PostgreSQL models, queries, and persistence for the bot.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, NaiveDate, Utc};
use deadpool_postgres::{Config as PoolConfig, Pool, Runtime};
use tokio_postgres::types::ToSql;
use tokio_postgres::NoTls;

use crate::config::{BridgeStyle, Config};

const LIVENESS_WINDOW: &str = "interval '30 minutes'";

/// Oldest/newest logged line. Uses each archive bucket's own min/max instead
/// of scanning `chat_messages`, which would decompress the whole archive.
const NEWEST_CHAT_AT: &str = "(SELECT max(t) FROM (
      SELECT max(received_at) AS t FROM chat_messages_raw
      UNION ALL SELECT max(max_at) FROM chat_archive) q)";
const OLDEST_CHAT_AT: &str = "(SELECT min(t) FROM (
      SELECT min(received_at) AS t FROM chat_messages_raw
      UNION ALL SELECT min(min_at) FROM chat_archive) q)";

/// Prefers the server's own broadcast online count (updates in ~1s) over the
/// roster count (can take minutes to catch up after a reconnect).
const ONLINE_COUNT_SQL: &str = "COALESCE(
      (SELECT reported_online FROM logger_heartbeats
        WHERE connected AND reported_online IS NOT NULL
          AND reported_online_at > now() - interval '30 seconds'
        ORDER BY reported_online_at DESC LIMIT 1),
      (SELECT count(*) FROM discord_online_now)
    )";

/// Sessions still open and still writing, within the last 30 minutes.
const LIVE_SESSIONS: &str = r#"
  SELECT DISTINCT written.session_id
  FROM (
    SELECT session_id, occurred_at AS at FROM player_events_raw
     WHERE occurred_at > now() - interval '30 minutes' AND session_id IS NOT NULL
    UNION ALL
    SELECT session_id, received_at FROM chat_messages_raw
     WHERE received_at > now() - interval '30 minutes' AND session_id IS NOT NULL
  ) written
  JOIN sessions live ON live.id = written.session_id
  WHERE live.ended_at IS NULL
"#;

/// Players currently visible to at least one live logger session.
const PRESENCE_VIEW: &str = r#"
  CREATE OR REPLACE VIEW discord_online_now AS
    SELECT DISTINCT ON (pp.server_host, lower(nd.name))
           pp.server_host, nd.name AS player_name, p.uuid AS player_uuid, pp.at AS occurred_at
    FROM player_presence pp
    JOIN name_dict nd ON nd.id = pp.player_id
    JOIN sessions s ON s.id = pp.session_id AND s.ended_at IS NULL
    LEFT JOIN players p ON p.name_id = pp.player_id
    WHERE pp.present
      AND pp.session_id IN (
  SELECT DISTINCT written.session_id
  FROM (
    SELECT session_id, occurred_at AS at FROM player_events_raw
     WHERE occurred_at > now() - interval '30 minutes' AND session_id IS NOT NULL
    UNION ALL
    SELECT session_id, received_at FROM chat_messages_raw
     WHERE received_at > now() - interval '30 minutes' AND session_id IS NOT NULL
  ) written
  JOIN sessions live ON live.id = written.session_id
  WHERE live.ended_at IS NULL
      )
    ORDER BY pp.server_host, lower(nd.name), pp.at DESC
"#;

mod models;
pub use models::*;

#[derive(Clone)]
pub struct Db {
    pools: Arc<HashMap<String, Pool>>,
    config: Arc<Config>,
}

impl Db {
    pub async fn connect(config: Arc<Config>) -> eyre::Result<Self> {
        let mut pools = HashMap::new();
        for server in &config.servers {
            let pool = create_pool(&server.database_url)?;
            pools.insert(server.key.clone(), pool);
        }
        Ok(Self {
            pools: Arc::new(pools),
            config,
        })
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn default_server_key(&self) -> &str {
        &self.config.servers[0].key
    }

    pub async fn ensure_command_channel_table(&self) -> eyre::Result<()> {
        let client = self.client(self.default_server_key()).await?;
        client
            .batch_execute(
                r#"CREATE TABLE IF NOT EXISTS discord_command_channel (
              id          BOOLEAN PRIMARY KEY DEFAULT true,
              channel_id  TEXT,
              updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
              CONSTRAINT discord_command_channel_one_row CHECK (id)
            )"#,
            )
            .await?;
        Ok(())
    }

    pub async fn get_command_channel(&self) -> eyre::Result<Option<String>> {
        self.ensure_command_channel_table().await?;
        let client = self.client(self.default_server_key()).await?;
        let row = client
            .query_opt(
                "SELECT channel_id FROM discord_command_channel WHERE id = true",
                &[],
            )
            .await?;
        Ok(row.and_then(|r| r.get(0)))
    }

    pub async fn set_command_channel(&self, channel_id: Option<&str>) -> eyre::Result<()> {
        self.ensure_command_channel_table().await?;
        let client = self.client(self.default_server_key()).await?;
        client
            .execute(
                r#"INSERT INTO discord_command_channel (id, channel_id, updated_at)
             VALUES (true, $1, now())
             ON CONFLICT (id) DO UPDATE SET channel_id = EXCLUDED.channel_id, updated_at = now()"#,
                &[&channel_id],
            )
            .await?;
        Ok(())
    }

    fn pool(&self, server_key: &str) -> eyre::Result<&Pool> {
        self.pools
            .get(server_key)
            .ok_or_else(|| eyre::eyre!("Unknown server \"{server_key}\""))
    }

    async fn client(&self, server_key: &str) -> eyre::Result<deadpool_postgres::Object> {
        Ok(self.pool(server_key)?.get().await?)
    }

    /// Loaded once at startup so heads resolve from memory.
    pub async fn all_player_uuids(&self, server_key: &str) -> eyre::Result<Vec<(String, String)>> {
        let client = self.client(server_key).await?;
        let rows = client
            .query(
                "SELECT lower(name), replace(uuid::text, '-', '') FROM players
                  WHERE uuid IS NOT NULL",
                &[],
            )
            .await?;
        Ok(rows.into_iter().map(|r| (r.get(0), r.get(1))).collect())
    }

    pub async fn ping_database(&self, server_key: &str) -> eyre::Result<u128> {
        let started = Instant::now();
        let client = self.client(server_key).await?;
        client.simple_query("SELECT 1").await?;
        Ok(started.elapsed().as_millis())
    }

    pub async fn ensure_bridge_state(&self, server_key: &str) -> eyre::Result<()> {
        let client = self.client(server_key).await?;
        client
            .batch_execute(
                r#"
            CREATE TABLE IF NOT EXISTS discord_bridge_state (
              id            BOOLEAN PRIMARY KEY DEFAULT true,
              last_chat_id  BIGINT NOT NULL DEFAULT 0,
              updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
              CONSTRAINT one_row CHECK (id)
            );
            ALTER TABLE discord_bridge_state ADD COLUMN IF NOT EXISTS channel_id TEXT;
            ALTER TABLE discord_bridge_state ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT true;
            ALTER TABLE discord_bridge_state ADD COLUMN IF NOT EXISTS last_event_id BIGINT NOT NULL DEFAULT 0;
            ALTER TABLE discord_bridge_state ADD COLUMN IF NOT EXISTS last_chat_at TIMESTAMPTZ;
            ALTER TABLE discord_bridge_state ADD COLUMN IF NOT EXISTS last_event_at TIMESTAMPTZ;
            ALTER TABLE discord_bridge_state ADD COLUMN IF NOT EXISTS last_presence_id BIGINT NOT NULL DEFAULT 0;
            ALTER TABLE discord_bridge_state ADD COLUMN IF NOT EXISTS last_presence_at TIMESTAMPTZ;
            ALTER TABLE discord_bridge_state ADD COLUMN IF NOT EXISTS style TEXT NOT NULL DEFAULT 'rich';
            ALTER TABLE discord_bridge_state ADD COLUMN IF NOT EXISTS rainbow BOOLEAN NOT NULL DEFAULT false;
            CREATE TABLE IF NOT EXISTS discord_online_peaks (
              day  DATE PRIMARY KEY,
              peak INTEGER     NOT NULL,
              at   TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE TABLE IF NOT EXISTS discord_event_routes (
              kind        TEXT PRIMARY KEY,
              enabled     BOOLEAN NOT NULL DEFAULT true,
              channel_id  TEXT,
              updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            -- logger_heartbeats is azalea-bot's table; added here too for ONLINE_COUNT_SQL.
            ALTER TABLE logger_heartbeats ADD COLUMN IF NOT EXISTS reported_online INTEGER;
            ALTER TABLE logger_heartbeats ADD COLUMN IF NOT EXISTS reported_online_at TIMESTAMPTZ;
        "#,
            )
            .await?;

        if let Err(err) = client.batch_execute(PRESENCE_VIEW).await {
            let missing = err
                .as_db_error()
                .map(|e| e.code().code() == "42P01")
                .unwrap_or(false);
            if missing {
                tracing::warn!(
                    "[db:{server_key}] logger tables are not there yet; presence will wait until they are"
                );
            } else {
                return Err(err.into());
            }
        }
        let _ = LIVENESS_WINDOW;
        self.ensure_event_routes(server_key, &self.config.bridge.kinds)
            .await?;
        Ok(())
    }

    pub async fn ensure_event_routes(
        &self,
        server_key: &str,
        default_enabled: &[String],
    ) -> eyre::Result<()> {
        let client = self.client(server_key).await?;
        client
            .batch_execute(
                r#"CREATE TABLE IF NOT EXISTS discord_event_routes (
              kind        TEXT PRIMARY KEY,
              enabled     BOOLEAN NOT NULL DEFAULT true,
              channel_id  TEXT,
              updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            )"#,
            )
            .await?;
        let enabled: HashSet<String> = if default_enabled.is_empty() {
            ROUTE_KINDS.iter().map(|k| (*k).to_string()).collect()
        } else {
            default_enabled
                .iter()
                .filter_map(|k| canonical_route_kind(k).map(|c| c.to_string()))
                .collect()
        };
        for kind in ROUTE_KINDS {
            let on = enabled.contains(*kind);
            client
                .execute(
                    r#"INSERT INTO discord_event_routes (kind, enabled, updated_at)
                       VALUES ($1, $2, now())
                       ON CONFLICT (kind) DO NOTHING"#,
                    &[&kind, &on],
                )
                .await?;
        }
        Ok(())
    }

    pub async fn get_event_routes(&self, server_key: &str) -> eyre::Result<Vec<EventRoute>> {
        self.ensure_event_routes(server_key, &[]).await?;
        let client = self.client(server_key).await?;
        let rows = client
            .query(
                "SELECT kind, enabled, channel_id FROM discord_event_routes ORDER BY kind",
                &[],
            )
            .await?;
        let by_kind: HashMap<String, (bool, Option<String>)> = rows
            .into_iter()
            .map(|r| (r.get(0), (r.get(1), r.get(2))))
            .collect();
        Ok(ROUTE_KINDS
            .iter()
            .map(|kind| {
                let (enabled, channel_id) = by_kind.get(*kind).cloned().unwrap_or((false, None));
                EventRoute {
                    kind: (*kind).to_string(),
                    enabled,
                    channel_id,
                }
            })
            .collect())
    }

    pub async fn set_event_route_enabled(
        &self,
        server_key: &str,
        kind: &str,
        enabled: bool,
    ) -> eyre::Result<()> {
        self.ensure_event_routes(server_key, &[]).await?;
        let client = self.client(server_key).await?;
        client
            .execute(
                r#"INSERT INTO discord_event_routes (kind, enabled, updated_at)
                   VALUES ($1, $2, now())
                   ON CONFLICT (kind) DO UPDATE SET enabled = EXCLUDED.enabled, updated_at = now()"#,
                &[&kind, &enabled],
            )
            .await?;
        Ok(())
    }

    pub async fn set_event_route_channel(
        &self,
        server_key: &str,
        kind: &str,
        channel_id: Option<&str>,
    ) -> eyre::Result<()> {
        self.ensure_event_routes(server_key, &[]).await?;
        let client = self.client(server_key).await?;
        client
            .execute(
                r#"INSERT INTO discord_event_routes (kind, channel_id, enabled, updated_at)
                   VALUES ($1, $2, true, now())
                   ON CONFLICT (kind) DO UPDATE SET
                     channel_id = EXCLUDED.channel_id,
                     enabled = true,
                     updated_at = now()"#,
                &[&kind, &channel_id],
            )
            .await?;
        Ok(())
    }

    pub async fn get_bridge_settings(&self, server_key: &str) -> eyre::Result<BridgeSettings> {
        let client = self.client(server_key).await?;
        let row = client
            .query_opt(
                "SELECT channel_id, enabled, style, rainbow FROM discord_bridge_state WHERE id = true",
                &[],
            )
            .await?;
        Ok(match row {
            Some(r) => BridgeSettings {
                channel_id: r.get(0),
                enabled: r.get(1),
                style: BridgeStyle::parse(r.get(2)),
                rainbow: r.get(3),
            },
            None => BridgeSettings {
                channel_id: None,
                enabled: true,
                style: BridgeStyle::Rich,
                rainbow: false,
            },
        })
    }

    pub async fn set_bridge_style(&self, server_key: &str, style: BridgeStyle) -> eyre::Result<()> {
        let client = self.client(server_key).await?;
        client
            .execute(
                r#"INSERT INTO discord_bridge_state (id, style, updated_at)
             VALUES (true, $1, now())
             ON CONFLICT (id) DO UPDATE SET style = EXCLUDED.style, updated_at = now()"#,
                &[&style.as_str()],
            )
            .await?;
        Ok(())
    }

    pub async fn set_bridge_rainbow(&self, server_key: &str, rainbow: bool) -> eyre::Result<()> {
        let client = self.client(server_key).await?;
        client
            .execute(
                r#"INSERT INTO discord_bridge_state (id, rainbow, updated_at)
             VALUES (true, $1, now())
             ON CONFLICT (id) DO UPDATE SET rainbow = EXCLUDED.rainbow, updated_at = now()"#,
                &[&rainbow],
            )
            .await?;
        Ok(())
    }

    pub async fn set_bridge_channel(
        &self,
        server_key: &str,
        channel_id: Option<&str>,
    ) -> eyre::Result<()> {
        let client = self.client(server_key).await?;
        client
            .execute(
                r#"INSERT INTO discord_bridge_state (id, channel_id, updated_at)
             VALUES (true, $1, now())
             ON CONFLICT (id) DO UPDATE SET channel_id = EXCLUDED.channel_id, updated_at = now()"#,
                &[&channel_id],
            )
            .await?;
        Ok(())
    }

    pub async fn set_bridge_enabled(&self, server_key: &str, enabled: bool) -> eyre::Result<()> {
        let client = self.client(server_key).await?;
        client
            .execute(
                r#"INSERT INTO discord_bridge_state (id, enabled, updated_at)
             VALUES (true, $1, now())
             ON CONFLICT (id) DO UPDATE SET enabled = EXCLUDED.enabled, updated_at = now()"#,
                &[&enabled],
            )
            .await?;
        Ok(())
    }

    pub async fn reset_event_routes(
        &self,
        server_key: &str,
        default_enabled: &[String],
    ) -> eyre::Result<()> {
        self.ensure_event_routes(server_key, default_enabled)
            .await?;
        let client = self.client(server_key).await?;
        let enabled: HashSet<String> = if default_enabled.is_empty() {
            ROUTE_KINDS.iter().map(|k| (*k).to_string()).collect()
        } else {
            default_enabled
                .iter()
                .filter_map(|k| canonical_route_kind(k).map(|c| c.to_string()))
                .collect()
        };
        for kind in ROUTE_KINDS {
            let on = enabled.contains(*kind);
            client
                .execute(
                    r#"INSERT INTO discord_event_routes (kind, enabled, channel_id, updated_at)
                       VALUES ($1, $2, NULL, now())
                       ON CONFLICT (kind) DO UPDATE SET
                         enabled = EXCLUDED.enabled,
                         channel_id = NULL,
                         updated_at = now()"#,
                    &[&kind, &on],
                )
                .await?;
        }
        Ok(())
    }

    pub async fn clear_bridge(&self, server_key: &str) -> eyre::Result<()> {
        let client = self.client(server_key).await?;
        client
            .execute(
                r#"INSERT INTO discord_bridge_state (id, channel_id, enabled, updated_at)
                   VALUES (true, NULL, false, now())
                   ON CONFLICT (id) DO UPDATE SET
                     channel_id = NULL,
                     enabled = false,
                     updated_at = now()"#,
                &[],
            )
            .await?;
        client
            .execute("DELETE FROM discord_event_routes", &[])
            .await?;
        Ok(())
    }

    pub async fn get_cursor(&self, server_key: &str) -> eyre::Result<(i64, Option<DateTime<Utc>>)> {
        let client = self.client(server_key).await?;
        let row = client
            .query_opt(
                "SELECT last_chat_id, last_chat_at FROM discord_bridge_state WHERE id = true",
                &[],
            )
            .await?;
        Ok(row
            .map(|r| (r.get::<_, i64>(0), r.get(1)))
            .unwrap_or((0, None)))
    }

    pub async fn set_cursor(
        &self,
        server_key: &str,
        id: i64,
        at: Option<DateTime<Utc>>,
    ) -> eyre::Result<()> {
        let client = self.client(server_key).await?;
        client
            .execute(
                r#"INSERT INTO discord_bridge_state (id, last_chat_id, last_chat_at, updated_at)
             VALUES (true, $1, $2, now())
             ON CONFLICT (id) DO UPDATE SET
               last_chat_id = EXCLUDED.last_chat_id,
               last_chat_at = COALESCE(EXCLUDED.last_chat_at, discord_bridge_state.last_chat_at),
               updated_at = now()"#,
                &[&id, &at],
            )
            .await?;
        Ok(())
    }

    pub async fn get_event_cursor(
        &self,
        server_key: &str,
    ) -> eyre::Result<(i64, Option<DateTime<Utc>>)> {
        let client = self.client(server_key).await?;
        let row = client
            .query_opt(
                "SELECT last_event_id, last_event_at FROM discord_bridge_state WHERE id = true",
                &[],
            )
            .await?;
        Ok(row
            .map(|r| (r.get::<_, i64>(0), r.get(1)))
            .unwrap_or((0, None)))
    }

    pub async fn set_event_cursor(
        &self,
        server_key: &str,
        id: i64,
        at: Option<DateTime<Utc>>,
    ) -> eyre::Result<()> {
        let client = self.client(server_key).await?;
        client
            .execute(
                r#"INSERT INTO discord_bridge_state (id, last_event_id, last_event_at, updated_at)
             VALUES (true, $1, $2, now())
             ON CONFLICT (id) DO UPDATE SET
               last_event_id = EXCLUDED.last_event_id,
               last_event_at = COALESCE(EXCLUDED.last_event_at, discord_bridge_state.last_event_at),
               updated_at = now()"#,
                &[&id, &at],
            )
            .await?;
        Ok(())
    }

    pub async fn get_presence_cursor(
        &self,
        server_key: &str,
    ) -> eyre::Result<(i64, Option<DateTime<Utc>>)> {
        let client = self.client(server_key).await?;
        let row = client
            .query_opt(
                "SELECT last_presence_id, last_presence_at FROM discord_bridge_state WHERE id = true",
                &[],
            )
            .await?;
        Ok(row
            .map(|r| (r.get::<_, i64>(0), r.get(1)))
            .unwrap_or((0, None)))
    }

    pub async fn set_presence_cursor(
        &self,
        server_key: &str,
        id: i64,
        at: Option<DateTime<Utc>>,
    ) -> eyre::Result<()> {
        let client = self.client(server_key).await?;
        client
            .execute(
                r#"INSERT INTO discord_bridge_state (id, last_presence_id, last_presence_at, updated_at)
             VALUES (true, $1, $2, now())
             ON CONFLICT (id) DO UPDATE SET
               last_presence_id = EXCLUDED.last_presence_id,
               last_presence_at = COALESCE(EXCLUDED.last_presence_at, discord_bridge_state.last_presence_at),
               updated_at = now()"#,
                &[&id, &at],
            )
            .await?;
        Ok(())
    }

    pub async fn latest_chat_id(&self, server_key: &str) -> eyre::Result<i64> {
        Ok(self.latest_chat_cursor(server_key).await?.0)
    }

    pub async fn latest_chat_cursor(
        &self,
        server_key: &str,
    ) -> eyre::Result<(i64, Option<DateTime<Utc>>)> {
        let client = self.client(server_key).await?;
        latest_cursor(
            &client,
            "SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END::bigint
               FROM chat_messages_id_seq",
            "SELECT max(received_at) FROM chat_messages_raw
              WHERE received_at > now() - CAST($1::text AS interval)",
        )
        .await
    }

    pub async fn count_backlog(
        &self,
        server_key: &str,
        after_id: i64,
        after_at: Option<DateTime<Utc>>,
        kinds: &[String],
    ) -> eyre::Result<i64> {
        let client = self.client(server_key).await?;
        let codes = EventKind::filter_codes(kinds);
        let row = client
            .query_one(
                // chat_messages_raw, not the view — avoids unpacking the archive on every poll.
                "SELECT count(*)::bigint FROM chat_messages_raw
                 WHERE id > $1 AND left(btrim(kind::text), 1) = ANY($2)
                   AND ($3::timestamptz IS NULL OR received_at >= $3 - interval '1 hour')",
                &[&after_id, &codes, &after_at],
            )
            .await?;
        Ok(row.get(0))
    }

    pub async fn latest_player_event_id(&self, server_key: &str) -> eyre::Result<i64> {
        Ok(self.latest_player_event_cursor(server_key).await?.0)
    }

    pub async fn latest_player_event_cursor(
        &self,
        server_key: &str,
    ) -> eyre::Result<(i64, Option<DateTime<Utc>>)> {
        let client = self.client(server_key).await?;
        latest_cursor(
            &client,
            "SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END::bigint
               FROM player_events_id_seq",
            "SELECT max(occurred_at) FROM player_events_raw
              WHERE occurred_at > now() - CAST($1::text AS interval)",
        )
        .await
    }

    pub async fn latest_presence_cursor(
        &self,
        server_key: &str,
    ) -> eyre::Result<(i64, Option<DateTime<Utc>>)> {
        let client = self.client(server_key).await?;
        latest_cursor(
            &client,
            "SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END::bigint
               FROM player_events_id_seq",
            r#"SELECT max(occurred_at) FROM player_events_raw
                WHERE event_type IN ('j'::"char", 'l'::"char")
                  AND occurred_at > now() - CAST($1::text AS interval)"#,
        )
        .await
    }

    /// Join/leave as `ChatRow`s — they are no longer stored in `chat_messages`.
    pub async fn fetch_new_presence_as_chat(
        &self,
        server_key: &str,
        after_id: i64,
        after_at: Option<DateTime<Utc>>,
        limit: i64,
    ) -> eyre::Result<Vec<ChatRow>> {
        let client = self.client(server_key).await?;
        let codes: Vec<String> = vec!["j".into(), "l".into()];
        let rows = client
            .query(
                r#"SELECT e.id, e.occurred_at, e.event_type::text, nd.name, s.server_host
                 FROM player_events_raw e
                 JOIN name_dict nd ON nd.id = e.player_id
                 LEFT JOIN sessions s ON s.id = e.session_id
                 WHERE e.id > $1 AND left(btrim(e.event_type::text), 1) = ANY($2)
                   -- 'r' = synthetic leave from a restart countdown; not real activity.
                   AND e.source <> 'r'
                   AND ($4::timestamptz IS NULL OR e.occurred_at >= $4 - interval '1 hour')
                 ORDER BY e.id
                 LIMIT $3"#,
                &[&after_id, &codes, &limit, &after_at],
            )
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                let event_type: String = r.get(2);
                let kind = presence_kind(&event_type)?.to_string();
                Some(ChatRow {
                    id: r.get(0),
                    received_at: r.get(1),
                    kind,
                    sender_name: None,
                    sender_label: None,
                    subject_name: Some(r.get(3)),
                    killer_name: None,
                    plain_text: String::new(),
                    server_host: r.get(4),
                })
            })
            .collect())
    }

    pub async fn fetch_new_messages(
        &self,
        server_key: &str,
        after_id: i64,
        after_at: Option<DateTime<Utc>>,
        kinds: &[String],
        limit: i64,
    ) -> eyre::Result<Vec<ChatRow>> {
        let client = self.client(server_key).await?;
        let codes = EventKind::filter_codes(kinds);
        let rows = client
            .query(
                r#"SELECT c.id, c.received_at, c.kind::text, snd.name, subj.name, kil.name,
            c.plain_text, s.server_host
  FROM chat_messages_raw c
  LEFT JOIN name_dict snd  ON snd.id  = c.sender_id
  LEFT JOIN name_dict subj ON subj.id = c.subject_id
  LEFT JOIN name_dict kil  ON kil.id  = c.killer_id
  LEFT JOIN sessions s ON s.id = c.session_id
  WHERE c.id > $1 AND left(btrim(c.kind::text), 1) = ANY($2)
    AND ($4::timestamptz IS NULL OR c.received_at >= $4 - interval '1 hour')
     ORDER BY c.id
     LIMIT $3"#,
                &[&after_id, &codes, &limit, &after_at],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| ChatRow {
                id: r.get(0),
                received_at: r.get(1),
                kind: r.get(2),
                sender_name: r.get(3),
                sender_label: None,
                subject_name: r.get(4),
                killer_name: r.get(5),
                plain_text: r.get(6),
                server_host: r.get(7),
            })
            .collect())
    }

    pub async fn ensure_watchbridge_tables(&self, server_key: &str) -> eyre::Result<()> {
        let client = self.client(server_key).await?;
        client
            .batch_execute(
                r#"CREATE TABLE IF NOT EXISTS discord_watchbridge_state (
              id          BOOLEAN PRIMARY KEY DEFAULT true,
              channel_id  TEXT,
              updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
              CONSTRAINT discord_watchbridge_state_one_row CHECK (id)
            );
            CREATE TABLE IF NOT EXISTS discord_watchbridge_players (
              player_name TEXT PRIMARY KEY,
              added_by    TEXT,
              created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            )"#,
            )
            .await?;
        Ok(())
    }

    pub async fn get_watchbridge_channel(&self, server_key: &str) -> eyre::Result<Option<String>> {
        self.ensure_watchbridge_tables(server_key).await?;
        let client = self.client(server_key).await?;
        let row = client
            .query_opt(
                "SELECT channel_id FROM discord_watchbridge_state WHERE id = true",
                &[],
            )
            .await?;
        Ok(row.and_then(|r| r.get(0)))
    }

    pub async fn set_watchbridge_channel(
        &self,
        server_key: &str,
        channel_id: Option<&str>,
    ) -> eyre::Result<()> {
        self.ensure_watchbridge_tables(server_key).await?;
        let client = self.client(server_key).await?;
        client
            .execute(
                r#"INSERT INTO discord_watchbridge_state (id, channel_id, updated_at)
             VALUES (true, $1, now())
             ON CONFLICT (id) DO UPDATE SET channel_id = EXCLUDED.channel_id, updated_at = now()"#,
                &[&channel_id],
            )
            .await?;
        Ok(())
    }

    pub async fn add_watchbridge_player(
        &self,
        server_key: &str,
        player_name: &str,
        added_by: &str,
    ) -> eyre::Result<()> {
        self.ensure_watchbridge_tables(server_key).await?;
        let client = self.client(server_key).await?;
        client
            .execute(
                r#"INSERT INTO discord_watchbridge_players (player_name, added_by)
                   VALUES ($1, $2)
                   ON CONFLICT (player_name) DO NOTHING"#,
                &[&player_name, &added_by],
            )
            .await?;
        Ok(())
    }

    /// Returns whether a matching player existed.
    pub async fn remove_watchbridge_player(
        &self,
        server_key: &str,
        player_name: &str,
    ) -> eyre::Result<bool> {
        self.ensure_watchbridge_tables(server_key).await?;
        let client = self.client(server_key).await?;
        let deleted = client
            .execute(
                "DELETE FROM discord_watchbridge_players WHERE lower(player_name) = lower($1)",
                &[&player_name],
            )
            .await?;
        Ok(deleted > 0)
    }

    pub async fn list_watchbridge_players(&self, server_key: &str) -> eyre::Result<Vec<String>> {
        self.ensure_watchbridge_tables(server_key).await?;
        let client = self.client(server_key).await?;
        let rows = client
            .query(
                "SELECT player_name FROM discord_watchbridge_players ORDER BY lower(player_name)",
                &[],
            )
            .await?;
        Ok(rows.into_iter().map(|r| r.get(0)).collect())
    }

    /// Join/leave events for watchlisted players, as `ChatRow`s.
    pub async fn fetch_watchbridge_hits(
        &self,
        server_key: &str,
        after_id: i64,
        after_at: Option<DateTime<Utc>>,
        limit: i64,
    ) -> eyre::Result<Vec<ChatRow>> {
        let client = self.client(server_key).await?;
        let codes: Vec<String> = vec!["j".into(), "l".into()];
        let rows = client
            .query(
                r#"SELECT e.id, e.occurred_at, e.event_type::text, nd.name, s.server_host
                 FROM player_events_raw e
                 JOIN name_dict nd ON nd.id = e.player_id
                 LEFT JOIN sessions s ON s.id = e.session_id
                 JOIN discord_watchbridge_players wp ON lower(wp.player_name) = lower(nd.name)
                 WHERE e.id > $1 AND left(btrim(e.event_type::text), 1) = ANY($2)
                   AND e.source <> 'r'
                   AND ($4::timestamptz IS NULL OR e.occurred_at >= $4 - interval '1 hour')
                 ORDER BY e.id
                 LIMIT $3"#,
                &[&after_id, &codes, &limit, &after_at],
            )
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                let event_type: String = r.get(2);
                let kind = presence_kind(&event_type)?.to_string();
                Some(ChatRow {
                    id: r.get(0),
                    received_at: r.get(1),
                    kind,
                    sender_name: None,
                    sender_label: None,
                    subject_name: Some(r.get(3)),
                    killer_name: None,
                    plain_text: String::new(),
                    server_host: r.get(4),
                })
            })
            .collect())
    }

    pub async fn overall_stats(&self, server_key: &str) -> eyre::Result<OverallStats> {
        let client = self.client(server_key).await?;
        let sql = format!(
            r#"
    SELECT (SELECT count(DISTINCT server_host) FROM sessions)::bigint                AS servers,
           (SELECT count(*) FROM sessions)::bigint                                   AS sessions,
           (SELECT s.messages FROM logger_stats s WHERE s.id)::bigint                    AS messages,
           (SELECT s.chat FROM logger_stats s WHERE s.id)::bigint                        AS chat,
           (SELECT count(*) FROM players)::bigint                                    AS players,
           (SELECT s.joins FROM logger_stats s WHERE s.id)::bigint                       AS joins,
           (SELECT s.leaves FROM logger_stats s WHERE s.id)::bigint                      AS leaves,
           (SELECT s.deaths FROM logger_stats s WHERE s.id)::bigint                      AS deaths,
           (SELECT s.kills FROM logger_stats s WHERE s.id)::bigint                       AS kills,
           (SELECT s.goals FROM logger_stats s WHERE s.id)::bigint                       AS goals,
           ({ONLINE_COUNT_SQL})::bigint                                             AS online,
           (SELECT min(started_at) FROM sessions)                                    AS first_seen,
           {NEWEST_CHAT_AT}                                                         AS last_seen
  "#
        );
        let r = client.query_one(&sql, &[]).await?;
        Ok(OverallStats {
            servers: r.get(0),
            sessions: r.get(1),
            messages: r.get(2),
            chat: r.get(3),
            players: r.get(4),
            joins: r.get(5),
            leaves: r.get(6),
            deaths: r.get(7),
            kills: r.get(8),
            goals: r.get(9),
            online: r.get(10),
            first_seen: r.get(11),
            last_seen: r.get(12),
        })
    }

    pub async fn leaderboard(
        &self,
        server_key: &str,
        metric: LeaderMetric,
        limit: i64,
        offset: i64,
    ) -> eyre::Result<Vec<LeaderRow>> {
        let client = self.client(server_key).await?;
        let rows = match metric {
            LeaderMetric::Kills => {
                client
                    .query(
                        r#"SELECT name, kills::float8 AS value, kills, deaths
         FROM kill_leaderboard
         WHERE kills > 0
         ORDER BY kills DESC, deaths ASC, name
         LIMIT $1 OFFSET $2"#,
                        &[&limit, &offset],
                    )
                    .await?
            }
            LeaderMetric::Kd => {
                let min = MIN_KILLS_FOR_KD;
                client
                    .query(
                        r#"SELECT name,
                round((kills::numeric / GREATEST(deaths, 1)), 2)::float8 AS value,
                kills, deaths
         FROM kill_leaderboard
         WHERE kills >= $3
         ORDER BY value DESC, kills DESC, name
         LIMIT $1 OFFSET $2"#,
                        &[&limit, &offset, &min],
                    )
                    .await?
            }
            LeaderMetric::Deaths => {
                client
                    .query(
                        r#"SELECT nd.name AS name, sum(d.deaths)::float8 AS value
         FROM player_daily d
         JOIN name_dict nd ON nd.id = d.player_id
         GROUP BY nd.name
         HAVING sum(d.deaths) > 0
         ORDER BY value DESC, name
         LIMIT $1 OFFSET $2"#,
                        &[&limit, &offset],
                    )
                    .await?
            }
            LeaderMetric::Joins => {
                client
                    .query(
                        r#"SELECT nd.name AS name, sum(d.joins)::float8 AS value
         FROM player_daily d
         JOIN name_dict nd ON nd.id = d.player_id
         GROUP BY nd.name
         HAVING sum(d.joins) > 0
         ORDER BY value DESC, name
         LIMIT $1 OFFSET $2"#,
                        &[&limit, &offset],
                    )
                    .await?
            }
            LeaderMetric::Messages => {
                client
                    .query(
                        r#"SELECT nd.name AS name, sum(d.messages)::float8 AS value
         FROM player_daily d
         JOIN name_dict nd ON nd.id = d.player_id
         GROUP BY nd.name
         HAVING sum(d.messages) > 0
         ORDER BY value DESC, name
         LIMIT $1 OFFSET $2"#,
                        &[&limit, &offset],
                    )
                    .await?
            }
            LeaderMetric::Playtime => {
                client
                    .query(
                        r#"SELECT nd.name AS name, sum(d.playtime_secs)::float8 AS value
         FROM player_daily d
         JOIN name_dict nd ON nd.id = d.player_id
         GROUP BY nd.name
         HAVING sum(d.playtime_secs) > 0
         ORDER BY value DESC, name
         LIMIT $1 OFFSET $2"#,
                        &[&limit, &offset],
                    )
                    .await?
            }
        };
        Ok(rows
            .into_iter()
            .map(|r| LeaderRow {
                name: r.get::<_, Option<String>>(0).unwrap_or_else(|| "?".into()),
                value: r.get(1),
                kills: r.try_get(2).ok(),
                deaths: r.try_get(3).ok(),
            })
            .collect())
    }

    pub async fn count_leaderboard(
        &self,
        server_key: &str,
        metric: LeaderMetric,
    ) -> eyre::Result<i64> {
        let client = self.client(server_key).await?;
        let sql = match metric {
            LeaderMetric::Kills => "SELECT count(*)::bigint FROM kill_leaderboard WHERE kills > 0",
            LeaderMetric::Kd => "SELECT count(*)::bigint FROM kill_leaderboard WHERE kills >= 5",
            LeaderMetric::Deaths => {
                r#"SELECT count(*)::bigint FROM (
                     SELECT player_id FROM player_daily
                     GROUP BY player_id HAVING sum(deaths) > 0) x"#
            }
            LeaderMetric::Joins => {
                r#"SELECT count(*)::bigint FROM (
                     SELECT player_id FROM player_daily
                     GROUP BY player_id HAVING sum(joins) > 0) x"#
            }
            LeaderMetric::Messages => {
                r#"SELECT count(*)::bigint FROM (
                     SELECT player_id FROM player_daily
                     GROUP BY player_id HAVING sum(messages) > 0) x"#
            }
            LeaderMetric::Playtime => {
                r#"SELECT count(*)::bigint FROM (
                     SELECT player_id FROM player_daily
                     GROUP BY player_id HAVING sum(playtime_secs) > 0) x"#
            }
        };
        let row = client.query_one(sql, &[]).await?;
        Ok(row.get(0))
    }

    pub async fn player_stats(
        &self,
        server_key: &str,
        name: &str,
    ) -> eyre::Result<Option<PlayerStats>> {
        let client = self.client(server_key).await?;
        let r = client
            .query_one(
                // Lifetime totals come from the player_daily rollup, not a full log scan.
                r#"WITH me AS (
              SELECT nd.id AS name_id, nd.name, p.uuid, p.chat_rank, p.first_seen, p.last_seen
              FROM name_dict nd
              LEFT JOIN players p ON p.name_id = nd.id
              WHERE lower(nd.name) = lower($1)
              LIMIT 1
            ),
            totals AS (
              SELECT COALESCE(sum(d.messages), 0)::bigint      AS messages,
                     COALESCE(sum(d.deaths), 0)::bigint        AS deaths,
                     COALESCE(sum(d.advancements), 0)::bigint  AS advancements,
                     COALESCE(sum(d.joins), 0)::bigint         AS joins,
                     COALESCE(sum(d.leaves), 0)::bigint        AS leaves,
                     COALESCE(sum(d.kills), 0)::bigint         AS kills,
                     COALESCE(sum(d.playtime_secs), 0)::float8 AS playtime_secs
              FROM player_daily d
              JOIN me ON me.name_id = d.player_id
            )
            SELECT COALESCE((SELECT name FROM me), $1::text)                        AS name,
                   (SELECT uuid::text FROM me)                                      AS uuid,
                   t.messages, t.deaths, t.advancements, t.joins, t.leaves, t.kills,
                   (SELECT first_seen FROM me)                                      AS first_seen,
                   (SELECT last_seen FROM me)                                       AS last_seen,
                   -- Name-pruned accessor keeps this to the few buckets that mention them.
                   (SELECT max(c.received_at) FROM chat_rows_for_name(
                        (SELECT name_id FROM me),
                        '-infinity'::timestamptz, 'infinity'::timestamptz) c
                     WHERE c.sender_id = (SELECT name_id FROM me)
                       AND c.kind IN ('c'::"char", 'w'::"char"))                    AS last_message_at,
                   (SELECT count(*) + 1 FROM kill_leaderboard k
                     WHERE k.kills > t.kills)::bigint                               AS kill_rank,
                   (SELECT chat_rank FROM me)                                       AS chat_rank,
                   t.playtime_secs
            FROM totals t"#,
                &[&name],
            )
            .await?;
        let stats = PlayerStats {
            name: r.get(0),
            uuid: r.get(1),
            messages: r.get(2),
            deaths: r.get(3),
            advancements: r.get(4),
            joins: r.get(5),
            leaves: r.get(6),
            kills: r.get(7),
            first_seen: r.get(8),
            last_seen: r.get(9),
            last_message_at: r.get(10),
            kill_rank: r.get(11),
            chat_rank: r.get(12),
            playtime_secs: r.get(13),
            session_count: 0,
        };
        if stats.messages == 0
            && stats.deaths == 0
            && stats.joins == 0
            && stats.leaves == 0
            && stats.kills == 0
            && stats.first_seen.is_none()
        {
            return Ok(None);
        }
        Ok(Some(stats))
    }

    pub async fn player_stats_window(
        &self,
        server_key: &str,
        name: &str,
        since_days: Option<i32>,
        until_days: Option<i32>,
    ) -> eyre::Result<PlayerStats> {
        let client = self.client(server_key).await?;
        // Counters come from the player_daily rollup, not a full log scan.
        let r = client
            .query_one(
                r#"WITH me AS (
    SELECT p.name, p.uuid, p.name_id, p.first_seen, p.last_seen
      FROM players p WHERE lower(p.name) = lower($1) LIMIT 1
  ),
  bounds AS (
    SELECT CASE WHEN $2::int IS NULL THEN NULL
                ELSE timezone('UTC', now())::date - ($2::int - 1) END AS lo,
           CASE WHEN $3::int IS NULL THEN NULL
                ELSE timezone('UTC', now())::date - $3::int END       AS hi
  ),
  d AS (
    SELECT pd.* FROM player_daily pd CROSS JOIN bounds b
     WHERE pd.player_id = (SELECT name_id FROM me)
       AND (b.lo IS NULL OR pd.day >= b.lo) AND (b.hi IS NULL OR pd.day <= b.hi)
  )
  SELECT COALESCE((SELECT name FROM me), $1::text)                   AS name,
         (SELECT uuid::text FROM me)                                 AS uuid,
         COALESCE((SELECT sum(messages)     FROM d), 0)::bigint      AS messages,
         COALESCE((SELECT sum(deaths)       FROM d), 0)::bigint      AS deaths,
         COALESCE((SELECT sum(advancements) FROM d), 0)::bigint      AS advancements,
         COALESCE((SELECT sum(joins)        FROM d), 0)::bigint      AS joins,
         COALESCE((SELECT sum(leaves)       FROM d), 0)::bigint      AS leaves,
         COALESCE((SELECT sum(kills)        FROM d), 0)::bigint      AS kills,
         (SELECT first_seen FROM me)                                 AS first_seen,
         (SELECT last_seen  FROM me)                                 AS last_seen,
         -- No rollup for this one; name+window pruning keeps it to a handful of buckets.
         (SELECT max(c.received_at) FROM chat_rows_for_name(
              (SELECT name_id FROM me),
              COALESCE((SELECT lo FROM bounds)::timestamp AT TIME ZONE 'UTC',
                       '-infinity'::timestamptz),
              COALESCE(((SELECT hi FROM bounds) + 1)::timestamp AT TIME ZONE 'UTC',
                       'infinity'::timestamptz)) c
           WHERE c.kind IN ('c'::"char", 'w'::"char")
             AND c.sender_id = (SELECT name_id FROM me))             AS last_message_at,
         COALESCE((SELECT sum(playtime_secs) FROM d), 0)::float8     AS playtime_secs"#,
                &[&name, &since_days, &until_days],
            )
            .await?;
        Ok(PlayerStats {
            name: r.get(0),
            uuid: r.get(1),
            messages: r.get(2),
            deaths: r.get(3),
            advancements: r.get(4),
            joins: r.get(5),
            leaves: r.get(6),
            kills: r.get(7),
            first_seen: r.get(8),
            last_seen: r.get(9),
            last_message_at: r.get(10),
            kill_rank: None,
            chat_rank: None,
            playtime_secs: r.get(11),
            session_count: r.get(5),
        })
    }

    pub async fn window_stats(
        &self,
        server_key: &str,
        since_days: Option<i32>,
        until_days: Option<i32>,
    ) -> eyre::Result<WindowStats> {
        let client = self.client(server_key).await?;
        // Daily rollups, not a log scan. Window is whole UTC days. Kills come from
        // player_daily (PvP-only) — stats_daily.kills includes mob kills.
        let r = client
            .query_one(
                r#"WITH bounds AS (
    SELECT CASE WHEN $1::int IS NULL THEN NULL
                ELSE timezone('UTC', now())::date - ($1::int - 1) END AS lo,
           CASE WHEN $2::int IS NULL THEN NULL
                ELSE timezone('UTC', now())::date - $2::int END       AS hi
  ),
  s AS (
    SELECT * FROM stats_daily, bounds b
     WHERE (b.lo IS NULL OR day >= b.lo) AND (b.hi IS NULL OR day <= b.hi)
  ),
  p AS (
    SELECT * FROM player_daily, bounds b
     WHERE (b.lo IS NULL OR day >= b.lo) AND (b.hi IS NULL OR day <= b.hi)
  )
  SELECT COALESCE((SELECT sum(chat)   FROM s), 0)::bigint,
         COALESCE((SELECT sum(joins)  FROM s), 0)::bigint,
         COALESCE((SELECT sum(leaves) FROM s), 0)::bigint,
         COALESCE((SELECT sum(deaths) FROM s), 0)::bigint,
         COALESCE((SELECT sum(kills)  FROM p), 0)::bigint,
         COALESCE((SELECT sum(goals)  FROM s), 0)::bigint,
         COALESCE((SELECT count(DISTINCT player_id) FROM p), 0)::bigint"#,
                &[&since_days, &until_days],
            )
            .await?;
        Ok(WindowStats {
            chat: r.get(0),
            joins: r.get(1),
            leaves: r.get(2),
            deaths: r.get(3),
            kills: r.get(4),
            goals: r.get(5),
            players: r.get(6),
        })
    }

    pub async fn player_aliases(
        &self,
        server_key: &str,
        name: &str,
    ) -> eyre::Result<Vec<(String, DateTime<Utc>)>> {
        let client = self.client(server_key).await?;
        let rows = client
            .query(
                r#"SELECT n.name, n.seen_at
     FROM player_names n
     WHERE n.uuid IN (SELECT uuid FROM players WHERE lower(name) = lower($1))
     ORDER BY n.seen_at DESC
     LIMIT 12"#,
                &[&name],
            )
            .await?;
        Ok(rows.into_iter().map(|r| (r.get(0), r.get(1))).collect())
    }

    pub async fn player_rivals(
        &self,
        server_key: &str,
        name: &str,
    ) -> eyre::Result<(Vec<RivalRow>, Vec<RivalRow>)> {
        let client = self.client(server_key).await?;
        // Join to `players` excludes mob kills (no player row for the killer).
        let victims = client
            .query(
                r#"WITH me AS (SELECT id FROM name_dict WHERE lower(name) = lower($1) LIMIT 1)
       SELECT nd.name AS name, count(*)::bigint AS count
       FROM chat_rows_for_name((SELECT id FROM me),
                               '-infinity'::timestamptz, 'infinity'::timestamptz) c
       JOIN name_dict nd ON nd.id = c.subject_id
       JOIN players   kp ON kp.name_id = c.killer_id
       WHERE c.kind = 'd'::"char" AND c.killer_id = (SELECT id FROM me)
       GROUP BY nd.name ORDER BY count DESC, name LIMIT 5"#,
                &[&name],
            )
            .await?;
        let nemeses = client
            .query(
                r#"WITH me AS (SELECT id FROM name_dict WHERE lower(name) = lower($1) LIMIT 1)
       SELECT nd.name AS name, count(*)::bigint AS count
       FROM chat_rows_for_name((SELECT id FROM me),
                               '-infinity'::timestamptz, 'infinity'::timestamptz) c
       JOIN name_dict nd ON nd.id = c.killer_id
       JOIN players   kp ON kp.name_id = c.killer_id
       WHERE c.kind = 'd'::"char" AND c.subject_id = (SELECT id FROM me)
       GROUP BY nd.name ORDER BY count DESC, name LIMIT 5"#,
                &[&name],
            )
            .await?;
        let map = |rows: Vec<tokio_postgres::Row>| {
            rows.into_iter()
                .map(|r| RivalRow {
                    name: r.get(0),
                    count: r.get(1),
                })
                .collect()
        };
        Ok((map(victims), map(nemeses)))
    }

    pub async fn player_servers(&self, server_key: &str, name: &str) -> eyre::Result<Vec<String>> {
        let client = self.client(server_key).await?;
        let rows = client
            .query(
                r#"WITH me AS (SELECT id FROM name_dict WHERE lower(name) = lower($1) LIMIT 1)
     SELECT DISTINCT s.server_host
     FROM event_rows_for_name((SELECT id FROM me),
                              '-infinity'::timestamptz, 'infinity'::timestamptz) e
     LEFT JOIN sessions s ON s.id = e.session_id
     WHERE s.server_host IS NOT NULL
     ORDER BY 1"#,
                &[&name],
            )
            .await?;
        Ok(rows.into_iter().map(|r| r.get(0)).collect())
    }

    pub async fn is_online(&self, server_key: &str, name: &str) -> eyre::Result<Option<String>> {
        let client = self.client(server_key).await?;
        let row = client
            .query_opt(
                "SELECT server_host FROM discord_online_now WHERE lower(player_name) = lower($1) LIMIT 1",
                &[&name],
            )
            .await?;
        Ok(row.map(|r| r.get(0)))
    }

    pub async fn search_player_names(
        &self,
        server_key: &str,
        prefix: &str,
        limit: i64,
    ) -> eyre::Result<Vec<String>> {
        let client = self.client(server_key).await?;
        let rows = client
            .query(
                r#"SELECT name FROM players
     WHERE $1 = '' OR lower(name) LIKE lower($1) || '%'
     ORDER BY last_seen DESC
     LIMIT $2"#,
                &[&prefix, &limit],
            )
            .await?;
        if !rows.is_empty() || prefix.is_empty() {
            return Ok(rows.into_iter().map(|r| r.get(0)).collect());
        }
        let loose = client
            .query(
                r#"SELECT name FROM players WHERE name ILIKE '%' || $1 || '%' ORDER BY last_seen DESC LIMIT $2"#,
                &[&prefix, &limit],
            )
            .await?;
        Ok(loose.into_iter().map(|r| r.get(0)).collect())
    }

    pub async fn search_players_across_servers(
        &self,
        prefix: &str,
        limit: usize,
        only_server_key: Option<&str>,
    ) -> Vec<PlayerHit> {
        let servers: Vec<_> = self
            .config
            .servers
            .iter()
            .filter(|s| only_server_key.map(|k| k == s.key).unwrap_or(true))
            .collect();
        let per_server: Vec<Vec<PlayerHit>> = futures::future::join_all(servers.iter().map(
            |server| async move {
                match self
                    .search_player_names(&server.key, prefix, limit as i64)
                    .await
                {
                    Ok(names) => names
                        .into_iter()
                        .map(|name| PlayerHit {
                            name,
                            server_key: server.key.clone(),
                            server_label: server.label.clone(),
                        })
                        .collect(),
                    Err(err) => {
                        tracing::error!("[db:{}] player search failed: {err}", server.key);
                        Vec::new()
                    }
                }
            },
        ))
        .await;
        let mut hits = Vec::new();
        let mut seen = HashSet::new();
        let mut index = 0usize;
        while hits.len() < limit {
            let mut added = false;
            for list in &per_server {
                if let Some(hit) = list.get(index) {
                    let key = format!("{}:{}", hit.server_key, hit.name.to_lowercase());
                    if seen.insert(key) {
                        hits.push(hit.clone());
                        added = true;
                        if hits.len() >= limit {
                            break;
                        }
                    }
                }
            }
            if !added {
                break;
            }
            index += 1;
        }
        hits
    }

    pub async fn servers_with_player(&self, name: &str) -> Vec<PlayerHit> {
        let hits = futures::future::join_all(self.config.servers.iter().map(|server| async move {
            match self.client(&server.key).await {
                Ok(client) => {
                    match client
                        .query_opt(
                            "SELECT name FROM players WHERE lower(name) = lower($1) LIMIT 1",
                            &[&name],
                        )
                        .await
                    {
                        Ok(Some(row)) => {
                            let found: String = row.get(0);
                            Some(PlayerHit {
                                name: found,
                                server_key: server.key.clone(),
                                server_label: server.label.clone(),
                            })
                        }
                        Ok(None) => None,
                        Err(err) => {
                            tracing::error!("[db:{}] player lookup failed: {err}", server.key);
                            None
                        }
                    }
                }
                Err(err) => {
                    tracing::error!("[db:{}] player lookup failed: {err}", server.key);
                    None
                }
            }
        }))
        .await;
        hits.into_iter().flatten().collect()
    }

    pub async fn chat_history(
        &self,
        server_key: &str,
        query: &ChatQuery,
    ) -> eyre::Result<Vec<ChatHistoryRow>> {
        let client = self.client(server_key).await?;
        let rows = client
            .query(
                // Text search needs the archive unpacked; name/time filters prune what they can.
                r#"SELECT c.received_at, nd.name AS sender_name, c.plain_text, s.server_host
     FROM chat_rows_for_name(
            (SELECT id FROM name_dict WHERE lower(name) = lower($1) LIMIT 1),
            CASE WHEN $5::int IS NULL THEN '-infinity'::timestamptz
                 ELSE now() - make_interval(days => $5) END,
            'infinity'::timestamptz) c
     LEFT JOIN name_dict nd ON nd.id = c.sender_id
     LEFT JOIN sessions s ON s.id = c.session_id
     WHERE c.kind = 'c'::"char"
       AND ($1::text IS NULL OR lower(nd.name) = lower($1))
       AND ($2::text IS NULL
            OR c.plain_text ILIKE '%' || $2 || '%')
     ORDER BY c.received_at DESC
     LIMIT $3 OFFSET $4"#,
                &[
                    &query.player.as_deref(),
                    &query.search.as_deref(),
                    &query.limit,
                    &query.offset,
                    &query.since_days,
                ],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| ChatHistoryRow {
                received_at: r.get(0),
                sender_name: r.get(1),
                plain_text: r.get(2),
                server_host: r.get(3),
            })
            .collect())
    }

    pub async fn count_chat_history(
        &self,
        server_key: &str,
        player: Option<&str>,
        search: Option<&str>,
        since_days: Option<i32>,
    ) -> eyre::Result<i64> {
        let client = self.client(server_key).await?;
        let row = client
            .query_one(
                r#"SELECT count(*)::bigint
     FROM chat_rows_for_name(
            (SELECT id FROM name_dict WHERE lower(name) = lower($1) LIMIT 1),
            CASE WHEN $3::int IS NULL THEN '-infinity'::timestamptz
                 ELSE now() - make_interval(days => $3) END,
            'infinity'::timestamptz) c
     LEFT JOIN name_dict nd ON nd.id = c.sender_id
     WHERE c.kind = 'c'::"char"
       AND ($1::text IS NULL OR lower(nd.name) = lower($1))
       AND ($2::text IS NULL
            OR c.plain_text ILIKE '%' || $2 || '%')"#,
                &[&player, &search, &since_days],
            )
            .await?;
        Ok(row.get(0))
    }

    /// Chat + whisper log for `/chat` and `/keyword`.
    pub async fn chat_log(
        &self,
        server_key: &str,
        query: &ChatQuery,
    ) -> eyre::Result<Vec<ChatHistoryRow>> {
        let client = self.client(server_key).await?;
        let rows = client
            .query(
                // Text search needs the archive unpacked; name/time filters prune what they can.
                r#"SELECT c.received_at, nd.name AS sender_name, c.plain_text, s.server_host
     FROM chat_rows_for_name(
            (SELECT id FROM name_dict WHERE lower(name) = lower($1) LIMIT 1),
            CASE WHEN $5::int IS NULL THEN '-infinity'::timestamptz
                 ELSE now() - make_interval(days => $5) END,
            'infinity'::timestamptz) c
     LEFT JOIN name_dict nd ON nd.id = c.sender_id
     LEFT JOIN sessions s ON s.id = c.session_id
     WHERE c.kind IN ('c'::"char", 'w'::"char")
       AND ($1::text IS NULL OR lower(nd.name) = lower($1))
       AND ($2::text IS NULL
            OR position(lower($2) in lower(c.plain_text)) > 0)
     ORDER BY c.received_at DESC
     LIMIT $3 OFFSET $4"#,
                &[
                    &query.player.as_deref(),
                    &query.search.as_deref(),
                    &query.limit,
                    &query.offset,
                    &query.since_days,
                ],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| ChatHistoryRow {
                received_at: r.get(0),
                sender_name: r.get(1),
                plain_text: r.get(2),
                server_host: r.get(3),
            })
            .collect())
    }

    pub async fn count_chat_log(
        &self,
        server_key: &str,
        player: Option<&str>,
        search: Option<&str>,
        since_days: Option<i32>,
    ) -> eyre::Result<i64> {
        let client = self.client(server_key).await?;
        let row = client
            .query_one(
                r#"SELECT count(*)::bigint
     FROM chat_rows_for_name(
            (SELECT id FROM name_dict WHERE lower(name) = lower($1) LIMIT 1),
            CASE WHEN $3::int IS NULL THEN '-infinity'::timestamptz
                 ELSE now() - make_interval(days => $3) END,
            'infinity'::timestamptz) c
     LEFT JOIN name_dict nd ON nd.id = c.sender_id
     WHERE c.kind IN ('c'::"char", 'w'::"char")
       AND ($1::text IS NULL OR lower(nd.name) = lower($1))
       AND ($2::text IS NULL
            OR position(lower($2) in lower(c.plain_text)) > 0)"#,
                &[&player, &search, &since_days],
            )
            .await?;
        Ok(row.get(0))
    }

    pub async fn count_online(&self, server_key: &str) -> eyre::Result<i64> {
        let client = self.client(server_key).await?;
        let sql = format!("SELECT ({ONLINE_COUNT_SQL})::bigint");
        let row = client.query_one(&sql, &[]).await?;
        Ok(row.get(0))
    }

    pub async fn topic_stats(&self, server_key: &str) -> eyre::Result<TopicStats> {
        let client = self.client(server_key).await?;
        let sql = format!(
            r#"
    SELECT (SELECT server_host FROM sessions ORDER BY started_at DESC LIMIT 1)   AS host,
           ({ONLINE_COUNT_SQL})::bigint                                         AS online,
           (SELECT peak FROM discord_online_peaks WHERE day = current_date)      AS peak_today,
           -- Physical tables, not the views — these windows never reach into the archive.
           (SELECT count(DISTINCT player_id) FROM player_events_raw
             WHERE occurred_at > now() - interval '24 hours')::bigint                    AS players_24h,
           (SELECT count(*) FROM chat_messages_raw
             WHERE kind IN ('c', 'w')
               AND received_at > now() - interval '1 hour')::bigint                      AS chat_1h,
           (SELECT count(*) FROM player_events_raw
             WHERE event_type = 'j'
               AND occurred_at > now() - interval '1 hour')::bigint                      AS joins_1h,
           (SELECT count(*) FROM chat_messages_raw
             WHERE kind = 'd'
               AND received_at > now() - interval '24 hours')::bigint                    AS deaths_24h,
           (SELECT count(*) > 0 FROM ({LIVE_SESSIONS}) live)                    AS logger_live,
           {NEWEST_CHAT_AT}                                                     AS last_message_at
  "#
        );
        let r = client.query_one(&sql, &[]).await?;
        let peak: Option<i32> = r.get(2);
        Ok(TopicStats {
            host: r.get(0),
            online: r.get(1),
            peak_today: peak.unwrap_or(0) as i64,
            players_24h: r.get(3),
            chat_1h: r.get(4),
            joins_1h: r.get(5),
            deaths_24h: r.get(6),
            logger_live: r.get(7),
            last_message_at: r.get(8),
        })
    }

    pub async fn record_online_peak(&self, server_key: &str, online: i64) -> eyre::Result<i64> {
        let client = self.client(server_key).await?;
        let online_i = online as i32;
        let row = client
            .query_one(
                r#"INSERT INTO discord_online_peaks (day, peak) VALUES (current_date, $1)
     ON CONFLICT (day) DO UPDATE
       SET peak = greatest(discord_online_peaks.peak, excluded.peak),
           at   = CASE WHEN excluded.peak > discord_online_peaks.peak THEN now()
                       ELSE discord_online_peaks.at END
     RETURNING peak"#,
                &[&online_i],
            )
            .await?;
        let peak: i32 = row.get(0);
        Ok(peak as i64)
    }

    pub async fn database_stats(&self, server_key: &str) -> eyre::Result<DatabaseStats> {
        let client = self.client(server_key).await?;
        let span_sql = format!("SELECT {OLDEST_CHAT_AT} AS oldest, {NEWEST_CHAT_AT} AS newest");
        let (meta, span, counts) = tokio::try_join!(
            client.query_one(
                r#"SELECT current_database() AS database,
              pg_size_pretty(pg_database_size(current_database())) AS size,
              pg_database_size(current_database())::bigint AS bytes"#,
                &[],
            ),
            client.query_one(&span_sql, &[]),
            client.query_one(
                r#"SELECT (SELECT s.chat FROM logger_stats s WHERE s.id)::bigint          AS chat,
              (SELECT s.joins FROM logger_stats s WHERE s.id)::bigint    AS joins,
              (SELECT s.leaves FROM logger_stats s WHERE s.id)::bigint   AS leaves,
              (SELECT s.deaths FROM logger_stats s WHERE s.id)::bigint         AS deaths,
              (SELECT s.kills FROM logger_stats s WHERE s.id)::bigint                               AS kills,
              (SELECT s.goals FROM logger_stats s WHERE s.id)::bigint   AS goals,
              (SELECT count(*) FROM players)::bigint                                    AS players,
              (SELECT count(*) FROM sessions)::bigint                                   AS sessions"#,
                &[],
            ),
        )?;
        Ok(DatabaseStats {
            database: meta.get(0),
            size: meta.get(1),
            bytes: meta.get(2),
            oldest: span.get(0),
            newest: span.get(1),
            chat: counts.get(0),
            joins: counts.get(1),
            leaves: counts.get(2),
            deaths: counts.get(3),
            kills: counts.get(4),
            goals: counts.get(5),
            players: counts.get(6),
            sessions: counts.get(7),
        })
    }

    pub async fn recent_events(
        &self,
        server_key: &str,
        kind: EventKind,
        limit: i64,
        since_days: Option<i32>,
    ) -> eyre::Result<Vec<EventRow>> {
        let client = self.client(server_key).await?;
        // Tries the hot window first; only falls back to the archive if that came up short.
        const HOT_CHAT: &str = "chat_messages_raw c";
        const ALL_CHAT: &str =
            "chat_rows_for_name(NULL::int, '-infinity'::timestamptz, 'infinity'::timestamptz) c";
        const HOT_EVENTS: &str = "player_events_raw e";
        const ALL_EVENTS: &str =
            "event_rows_for_name(NULL::int, '-infinity'::timestamptz, 'infinity'::timestamptz) e";

        let codes: Vec<&str> = kind.db_codes().to_vec();
        let (body, params): (&str, Vec<&(dyn ToSql + Sync)>) = match kind {
            EventKind::Kill => (
                r#"SELECT c.received_at AS occurred_at, kil.name AS player_name,
              subj.name AS detail
       FROM {SRC}
       JOIN name_dict kil  ON kil.id  = c.killer_id
       JOIN name_dict subj ON subj.id = c.subject_id
       JOIN players   kp   ON kp.name_id = c.killer_id
       WHERE c.kind = 'd'::"char"
         AND CASE WHEN $2::int IS NULL THEN true ELSE c.received_at >= now() - make_interval(days => $2) END
       ORDER BY c.received_at DESC LIMIT $1"#,
                vec![&limit, &since_days],
            ),
            EventKind::Chat => (
                r#"SELECT c.received_at AS occurred_at, nd.name AS player_name,
              c.plain_text AS detail
       FROM {SRC}
       JOIN name_dict nd ON nd.id = c.sender_id
       WHERE c.kind = 'c'::"char"
         AND CASE WHEN $2::int IS NULL THEN true ELSE c.received_at >= now() - make_interval(days => $2) END
       ORDER BY c.received_at DESC LIMIT $1"#,
                vec![&limit, &since_days],
            ),
            EventKind::Death | EventKind::Advancement => (
                r#"SELECT c.received_at AS occurred_at,
              COALESCE(subj.name, '?') AS player_name,
              CASE
                WHEN c.kind = 'd'::"char" THEN
                  CASE
                    WHEN btrim(COALESCE(c.plain_text, '')) <> ''
                         AND position(COALESCE(subj.name, '') in c.plain_text) = 1
                      THEN c.plain_text
                    WHEN kil.name IS NOT NULL AND btrim(kil.name) <> ''
                      THEN 'was slain by ' || kil.name
                    ELSE 'died'
                  END
                ELSE
                  CASE
                    WHEN btrim(COALESCE(c.plain_text, '')) <> ''
                         AND position(COALESCE(subj.name, '') in c.plain_text) = 1
                      THEN c.plain_text
                    WHEN btrim(COALESCE(c.plain_text, '')) <> ''
                      THEN 'made the advancement ' || c.plain_text
                    ELSE 'made an advancement'
                  END
              END AS detail
       FROM {SRC}
       LEFT JOIN name_dict subj ON subj.id = c.subject_id
       LEFT JOIN name_dict kil  ON kil.id  = c.killer_id
       WHERE left(btrim(c.kind::text), 1) = ANY($1::text[])
         AND CASE WHEN $3::int IS NULL THEN true ELSE c.received_at >= now() - make_interval(days => $3) END
       ORDER BY c.received_at DESC LIMIT $2"#,
                vec![&codes, &limit, &since_days],
            ),
            EventKind::Join | EventKind::Leave => (
                r#"SELECT e.occurred_at, nd.name AS player_name, e.source::text AS detail
     FROM {SRC}
     JOIN name_dict nd ON nd.id = e.player_id
     WHERE left(btrim(e.event_type::text), 1) = ANY($1::text[])
       AND CASE WHEN $3::int IS NULL THEN true ELSE e.occurred_at >= now() - make_interval(days => $3) END
     ORDER BY e.occurred_at DESC LIMIT $2"#,
                vec![&codes, &limit, &since_days],
            ),
        };
        let (hot_src, all_src) = match kind {
            EventKind::Join | EventKind::Leave => (HOT_EVENTS, ALL_EVENTS),
            _ => (HOT_CHAT, ALL_CHAT),
        };
        let mut rows = client
            .query(&body.replace("{SRC}", hot_src), &params)
            .await?;
        if (rows.len() as i64) < limit {
            rows = client
                .query(&body.replace("{SRC}", all_src), &params)
                .await?;
        }
        Ok(rows
            .into_iter()
            .map(|r| EventRow {
                occurred_at: r.get(0),
                player_name: r.get::<_, Option<String>>(1).unwrap_or_else(|| "?".into()),
                detail: r.get(2),
            })
            .collect())
    }

    pub async fn player_deaths(
        &self,
        server_key: &str,
        name: &str,
        limit: i64,
        since_days: Option<i32>,
    ) -> eyre::Result<Vec<EventRow>> {
        let client = self.client(server_key).await?;
        let rows = client
            .query(
                r#"WITH me AS (SELECT id FROM name_dict WHERE lower(name) = lower($1) LIMIT 1)
     SELECT c.received_at AS occurred_at, COALESCE(subj.name, $1) AS player_name,
       CASE
         WHEN btrim(COALESCE(c.plain_text, '')) <> ''
              AND position(COALESCE(subj.name, $1) in c.plain_text) = 1
           THEN c.plain_text
         WHEN kil.name IS NOT NULL AND btrim(kil.name) <> ''
           THEN 'was slain by ' || kil.name
         ELSE 'died'
       END AS detail
     FROM chat_rows_for_name((SELECT id FROM me),
            CASE WHEN $3::int IS NULL THEN '-infinity'::timestamptz
                 ELSE now() - make_interval(days => $3) END,
            'infinity'::timestamptz) c
     LEFT JOIN name_dict subj ON subj.id = c.subject_id
     LEFT JOIN name_dict kil  ON kil.id  = c.killer_id
     WHERE c.kind = 'd'::"char" AND c.subject_id = (SELECT id FROM me)
     ORDER BY c.received_at DESC LIMIT $2"#,
                &[&name, &limit, &since_days],
            )
            .await?;
        Ok(map_events(rows))
    }

    pub async fn player_kill_feed(
        &self,
        server_key: &str,
        name: &str,
        limit: i64,
        since_days: Option<i32>,
    ) -> eyre::Result<Vec<EventRow>> {
        let client = self.client(server_key).await?;
        let rows = client
            .query(
                r#"WITH me AS (SELECT id FROM name_dict WHERE lower(name) = lower($1) LIMIT 1)
     SELECT c.received_at AS occurred_at, kil.name AS player_name, subj.name AS detail
     FROM chat_rows_for_name((SELECT id FROM me),
            CASE WHEN $3::int IS NULL THEN '-infinity'::timestamptz
                 ELSE now() - make_interval(days => $3) END,
            'infinity'::timestamptz) c
     JOIN name_dict kil  ON kil.id  = c.killer_id
     JOIN name_dict subj ON subj.id = c.subject_id
     JOIN players   kp   ON kp.name_id = c.killer_id
     WHERE c.kind = 'd'::"char" AND c.killer_id = (SELECT id FROM me)
     ORDER BY c.received_at DESC LIMIT $2"#,
                &[&name, &limit, &since_days],
            )
            .await?;
        Ok(map_events(rows))
    }

    pub async fn player_join_leave(
        &self,
        server_key: &str,
        name: &str,
        event_type: &str,
        limit: i64,
        since_days: Option<i32>,
    ) -> eyre::Result<Vec<EventRow>> {
        let client = self.client(server_key).await?;
        let types: Vec<&str> = match event_type {
            "join" | "j" => vec!["join", "j"],
            "leave" | "l" => vec!["leave", "l"],
            other => vec![other],
        };
        let rows = client
            .query(
                r#"WITH me AS (SELECT id FROM name_dict WHERE lower(name) = lower($1) LIMIT 1)
     SELECT e.occurred_at, nd.name AS player_name, e.source::text AS detail
     FROM event_rows_for_name((SELECT id FROM me),
            CASE WHEN $4::int IS NULL THEN '-infinity'::timestamptz
                 ELSE now() - make_interval(days => $4) END,
            'infinity'::timestamptz) e
     JOIN name_dict nd ON nd.id = e.player_id
     WHERE left(btrim(e.event_type::text), 1) = ANY($2::text[])
     ORDER BY e.occurred_at DESC LIMIT $3"#,
                &[&name, &types, &limit, &since_days],
            )
            .await?;
        Ok(map_events(rows))
    }

    pub async fn player_advancements(
        &self,
        server_key: &str,
        name: &str,
        limit: i64,
        since_days: Option<i32>,
    ) -> eyre::Result<Vec<EventRow>> {
        let client = self.client(server_key).await?;
        let rows = client
            .query(
                r#"WITH me AS (SELECT id FROM name_dict WHERE lower(name) = lower($1) LIMIT 1)
     SELECT c.received_at AS occurred_at, COALESCE(subj.name, $1) AS player_name,
       CASE
         WHEN btrim(COALESCE(c.plain_text, '')) <> ''
              AND position(COALESCE(subj.name, $1) in c.plain_text) = 1
           THEN c.plain_text
         WHEN btrim(COALESCE(c.plain_text, '')) <> ''
           THEN 'made the advancement ' || c.plain_text
         ELSE 'made an advancement'
       END AS detail
     FROM chat_rows_for_name((SELECT id FROM me),
            CASE WHEN $3::int IS NULL THEN '-infinity'::timestamptz
                 ELSE now() - make_interval(days => $3) END,
            'infinity'::timestamptz) c
     LEFT JOIN name_dict subj ON subj.id = c.subject_id
     WHERE c.kind = 'a'::"char" AND c.subject_id = (SELECT id FROM me)
     ORDER BY c.received_at DESC LIMIT $2"#,
                &[&name, &limit, &since_days],
            )
            .await?;
        Ok(map_events(rows))
    }

    pub async fn last_message_at(&self, server_key: &str) -> eyre::Result<Option<DateTime<Utc>>> {
        let client = self.client(server_key).await?;
        let row = client
            .query_one(&format!("SELECT {NEWEST_CHAT_AT} AS at"), &[])
            .await?;
        Ok(row.get(0))
    }
}

fn map_events(rows: Vec<tokio_postgres::Row>) -> Vec<EventRow> {
    rows.into_iter()
        .map(|r| EventRow {
            occurred_at: r.get(0),
            player_name: r.get::<_, Option<String>>(1).unwrap_or_else(|| "?".into()),
            detail: r.get(2),
        })
        .collect()
}

/// Newest (id, timestamp) in a log table. Uses the sequence for the id (no
/// btree on `id` to scan) — must be >= the true max, or old rows replay into Discord.
async fn latest_cursor(
    client: &deadpool_postgres::Object,
    id_sql: &str,
    newest_ts: &str,
) -> eyre::Result<(i64, Option<DateTime<Utc>>)> {
    let id: i64 = client.query_one(id_sql, &[]).await?.get(0);
    for window in ["1 day", "30 days", "1 year"] {
        let at: Option<DateTime<Utc>> = client.query_one(newest_ts, &[&window]).await?.get(0);
        if at.is_some() {
            return Ok((id, at));
        }
    }
    Ok((id, None))
}

pub fn create_pool(database_url: &str) -> eyre::Result<Pool> {
    let mut cfg = PoolConfig::new();
    cfg.url = Some(database_url.to_string());
    cfg.pool = Some(deadpool_postgres::PoolConfig {
        max_size: 4,
        ..Default::default()
    });
    Ok(cfg.create_pool(Some(Runtime::Tokio1), NoTls)?)
}

// silence unused import warnings for types used by callers
#[allow(dead_code)]
fn _keep(_: NaiveDate, _: &dyn ToSql) {}
