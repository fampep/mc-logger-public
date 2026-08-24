use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};

use crate::cidr::display_ip;
use crate::config::RescanPriority;
use crate::ping::status_ping;
use crate::{RateLimiter, Store, process_ping, ScanConfig};

pub struct RescanConfig {
    pub timeout: Duration,
    pub concurrency: usize,
    pub rate: u32,
    pub priority: RescanPriority,
    pub limit: Option<usize>,
}

#[derive(Debug, Default)]
pub struct RescanStats {
    pub probed: usize,
    pub matched: usize,
    pub new_rows: usize,
}

pub async fn run_rescan(
    store: &Store,
    rescan: &RescanConfig,
    scan: &ScanConfig,
) -> Result<RescanStats> {
    let servers = store.list_servers(rescan.priority, rescan.limit).await?;
    let total = servers.len();
    if total == 0 {
        eprintln!("rescanner: no servers in database");
        return Ok(RescanStats::default());
    }

    eprintln!(
        "rescanner: re-probing {} server(s) (concurrency {}, rate {}/s)",
        total,
        rescan.concurrency,
        if rescan.rate == 0 {
            "unlimited".to_string()
        } else {
            rescan.rate.to_string()
        }
    );

    let semaphore = Arc::new(Semaphore::new(rescan.concurrency));
    let rate_state = Arc::new(Mutex::new(RateLimiter::new(rescan.rate)));
    let mut handles = Vec::with_capacity(total);

    for (ip, port) in servers {
        let permit = semaphore.clone().acquire_owned().await?;
        let store = store.clone_store();
        let scan = scan.clone();
        let timeout = rescan.timeout;
        let rate_state = rate_state.clone();
        handles.push(tokio::spawn(async move {
            let _permit = permit;
            rate_state.lock().await.wait().await;
            let ip_str = display_ip(ip);
            let ping = status_ping(&ip_str, port, timeout).await?;
            process_ping(&store, ip, port, ping, &scan, None).await
        }));
    }

    let mut stats = RescanStats {
        probed: total,
        ..Default::default()
    };
    for handle in handles {
        match handle.await? {
            Ok((matched, inserted)) => {
                if matched {
                    stats.matched += 1;
                }
                stats.new_rows += inserted;
            }
            Err(_) => {}
        }
    }

    eprintln!(
        "rescanner: done, {} matched, {} new rows",
        stats.matched, stats.new_rows
    );
    Ok(stats)
}
