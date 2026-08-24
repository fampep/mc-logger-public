use anyhow::{Context, Result};
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::types::Json;
use tokio_postgres::{Client, NoTls};

use crate::config::RescanPriority;

pub struct PostgresStore {
    client: Arc<Mutex<Client>>,
}

impl PostgresStore {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(database_url, NoTls)
            .await
            .context("connect to postgres")?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("postgres connection error: {error}");
            }
        });
        let store = Self {
            client: Arc::new(Mutex::new(client)),
        };
        store.init_schema().await?;
        Ok(store)
    }

    async fn init_schema(&self) -> Result<()> {
        let sql = include_str!("../schema.sql");
        let client = self.client.lock().await;
        client.batch_execute(sql).await.context("apply postgres schema")?;
        Ok(())
    }

    pub async fn list_servers(
        &self,
        priority: RescanPriority,
        limit: Option<usize>,
    ) -> Result<Vec<(String, u16)>> {
        let order = match priority {
            RescanPriority::OldestFirst => "ORDER BY last_seen ASC",
            RescanPriority::NewestFirst => "ORDER BY last_seen DESC",
        };
        let sql = match limit {
            Some(n) => format!(
                "SELECT host(ip)::text, port FROM discovered_servers {order} LIMIT {n}"
            ),
            None => format!("SELECT host(ip)::text, port FROM discovered_servers {order}"),
        };
        let client = self.client.lock().await;
        let rows = client.query(&sql, &[]).await?;
        Ok(rows
            .iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, i32>(1) as u16))
            .collect())
    }

    pub async fn upsert_finding(
        &self,
        ip: &str,
        port: i32,
        hostname: Option<&str>,
        motd: &str,
        version_name: &str,
        protocol: i32,
        players_online: i32,
        players_max: i32,
        matched_query: Option<&str>,
        scan_source: &str,
        raw_json: &str,
    ) -> Result<UpsertResult> {
        let ip_addr: IpAddr = ip.parse().context("invalid ip for postgres inet")?;
        let raw_json_value: Json<serde_json::Value> = Json(
            serde_json::from_str(raw_json).unwrap_or_else(|_| serde_json::json!({})),
        );
        let client = self.client.lock().await;
        let row = client
            .query_one(
                "
                INSERT INTO discovered_servers (
                    ip, port, hostname, motd_plain, version_name, protocol,
                    players_online, players_max, matched_query, scan_source, raw_json
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (ip, port) DO UPDATE SET
                    hostname = COALESCE(EXCLUDED.hostname, discovered_servers.hostname),
                    motd_plain = EXCLUDED.motd_plain,
                    version_name = EXCLUDED.version_name,
                    protocol = EXCLUDED.protocol,
                    players_online = EXCLUDED.players_online,
                    players_max = EXCLUDED.players_max,
                    matched_query = COALESCE(EXCLUDED.matched_query, discovered_servers.matched_query),
                    last_seen = now(),
                    scan_source = EXCLUDED.scan_source,
                    raw_json = EXCLUDED.raw_json
                RETURNING id, (xmax = 0) AS is_new
                ",
                &[
                    &ip_addr,
                    &port,
                    &hostname,
                    &motd,
                    &version_name,
                    &protocol,
                    &players_online,
                    &players_max,
                    &matched_query,
                    &scan_source,
                    &raw_json_value,
                ],
            )
            .await?;

        Ok(UpsertResult {
            id: row.get(0),
            is_new: row.get(1),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct UpsertResult {
    pub id: i64,
    pub is_new: bool,
}

