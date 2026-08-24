use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{Connection, params};
use std::path::Path;

use crate::config::RescanPriority;

pub struct SqliteStore {
    conn: Connection,
}

impl SqliteStore {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
            }
        }
        let conn = Connection::open(path)
            .with_context(|| format!("open sqlite db at {}", path.display()))?;
        let db = Self { conn };
        db.init_schema()?;
        Ok(db)
    }

    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS findings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ip TEXT NOT NULL,
                port INTEGER NOT NULL,
                motd TEXT NOT NULL DEFAULT '',
                version_name TEXT NOT NULL DEFAULT '',
                protocol INTEGER NOT NULL DEFAULT 0,
                players_online INTEGER NOT NULL DEFAULT 0,
                players_max INTEGER NOT NULL DEFAULT 0,
                ping_hostname TEXT,
                matched_query TEXT,
                scan_source TEXT NOT NULL DEFAULT 'manual',
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                UNIQUE(ip, port, matched_query)
            );
            CREATE INDEX IF NOT EXISTS idx_findings_query ON findings(matched_query);
            CREATE INDEX IF NOT EXISTS idx_findings_last_seen ON findings(last_seen);
            ",
        )?;
        Ok(())
    }

    pub fn list_servers(
        &self,
        priority: RescanPriority,
        limit: Option<usize>,
    ) -> Result<Vec<(String, u16)>> {
        let order = match priority {
            RescanPriority::OldestFirst => "ORDER BY last_seen ASC",
            RescanPriority::NewestFirst => "ORDER BY last_seen DESC",
        };
        let sql = match limit {
            Some(n) => format!("SELECT DISTINCT ip, port FROM findings {order} LIMIT {n}"),
            None => format!("SELECT DISTINCT ip, port FROM findings {order}"),
        };
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, u16>(1)?))
        })?;
        rows.collect::<Result<Vec<_>, _>>().context("list servers")
    }

    pub fn insert_finding(
        &self,
        ip: &str,
        port: u16,
        motd: &str,
        version_name: &str,
        protocol: i32,
        players_online: u32,
        players_max: u32,
        ping_hostname: Option<&str>,
        matched_query: Option<&str>,
        scan_source: &str,
        raw_json: &str,
    ) -> Result<bool> {
        let now = Utc::now().to_rfc3339();
        let changed = self.conn.execute(
            "
            INSERT INTO findings (
                ip, port, motd, version_name, protocol,
                players_online, players_max, ping_hostname,
                matched_query, scan_source, first_seen, last_seen, raw_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?11, ?12)
            ON CONFLICT(ip, port, matched_query) DO UPDATE SET
                motd = excluded.motd,
                version_name = excluded.version_name,
                protocol = excluded.protocol,
                players_online = excluded.players_online,
                players_max = excluded.players_max,
                ping_hostname = excluded.ping_hostname,
                scan_source = excluded.scan_source,
                last_seen = excluded.last_seen,
                raw_json = excluded.raw_json
            ",
            params![
                ip,
                port,
                motd,
                version_name,
                protocol,
                players_online,
                players_max,
                ping_hostname,
                matched_query,
                scan_source,
                now,
                raw_json,
            ],
        )?;
        Ok(changed > 0)
    }
}
