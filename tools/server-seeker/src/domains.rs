use anyhow::{Context, Result, bail};
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};

use crate::cidr::display_ip;
use crate::ping::status_ping;
use crate::{RateLimiter, Store, process_ping, ScanConfig};

/// Resolve a hostname to IP addresses (A/AAAA).
pub fn resolve_domain(host: &str, port: u16) -> Result<Vec<IpAddr>> {
    let host = host.trim();
    let addrs: Vec<IpAddr> = format!("{host}:{port}")
        .to_socket_addrs()
        .with_context(|| format!("DNS lookup failed for {host}"))?
        .map(|sa: SocketAddr| sa.ip())
        .collect();
    if addrs.is_empty() {
        bail!("no addresses for {host}");
    }
    Ok(addrs)
}

pub struct DomainScanConfig {
    pub domains: Vec<String>,
    pub port: u16,
    pub timeout: Duration,
    pub concurrency: usize,
    pub rate: u32,
}

pub async fn run_domains(
    store: &Store,
    domains: &DomainScanConfig,
    scan: &ScanConfig,
) -> Result<(usize, usize)> {
    if domains.domains.is_empty() {
        return Ok((0, 0));
    }

    eprintln!(
        "domains: scanning {} hostname(s) on port {} (concurrency {}, rate {}/s)",
        domains.domains.len(),
        domains.port,
        domains.concurrency,
        domains.rate
    );

    let semaphore = Arc::new(Semaphore::new(domains.concurrency));
    let rate_state = Arc::new(Mutex::new(RateLimiter::new(domains.rate)));
    let mut handles = Vec::new();

    for domain in &domains.domains {
        let domain = domain.clone();
        let targets = collect_domain_targets(&domain, domains.port)?;
        eprintln!(
            "domains: {domain} ??? {} target(s)",
            targets.len()
        );

        for (connect_host, ip) in targets {
            let permit = semaphore.clone().acquire_owned().await?;
            let store = store.clone_store();
            let scan = scan.clone();
            let rate_state = rate_state.clone();
            let timeout = domains.timeout;
            let port = domains.port;
            let hostname = domain.clone();
            handles.push(tokio::spawn(async move {
                let _permit = permit;
                rate_state.lock().await.wait().await;
                let ping = match status_ping(&connect_host, port, timeout).await {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!(
                            "domains: {connect_host}:{port} ping failed: {e:#}"
                        );
                        return Ok((false, 0));
                    }
                };
                process_ping(
                    &store,
                    ip,
                    port,
                    ping,
                    &scan,
                    Some(&hostname),
                )
                .await
            }));
        }
    }

    let mut matched = 0usize;
    let mut new_count = 0usize;
    for handle in handles {
        match handle.await {
            Ok(Ok((is_match, inserted))) => {
                if is_match {
                    matched += 1;
                }
                new_count += inserted;
            }
            Ok(Err(error)) => {
                eprintln!("domains: store failed: {error:#}");
            }
            Err(error) => {
                eprintln!("domains: task join failed: {error}");
            }
        }
    }

    eprintln!("domains: done, {matched} matched, {new_count} new");
    Ok((matched, new_count))
}

/// Targets: (connect host for handshake, canonical IP for storage).
fn collect_domain_targets(domain: &str, port: u16) -> Result<Vec<(String, IpAddr)>> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    let mut push = |connect: &str, ip: IpAddr| {
        if seen.insert((display_ip(ip), port)) {
            out.push((connect.to_string(), ip));
        }
    };

    if let Ok(ips) = resolve_domain(domain, port) {
        for ip in ips {
            push(domain, ip);
            push(&display_ip(ip), ip);
        }
        return Ok(out);
    }

    // Fallback: domain may be a literal IP or reachable without DNS expansion.
    if let Ok(ip) = domain.parse::<IpAddr>() {
        push(domain, ip);
    }

    if out.is_empty() {
        bail!("could not resolve {domain}");
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedupes_domain_targets() {
        let targets = collect_domain_targets("127.0.0.1", 25565).unwrap();
        assert!(!targets.is_empty());
        let unique: HashSet<_> = targets.iter().map(|(_, ip)| *ip).collect();
        assert_eq!(unique.len(), targets.len());
    }
}

