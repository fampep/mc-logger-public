use anyhow::{Context, Result, bail};
use std::net::Ipv4Addr;
use std::path::Path;
use std::str::FromStr;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

/// Parsed masscan stdout line: `Discovered open port 25565/tcp on 1.2.3.4`
pub fn parse_masscan_line(line: &str) -> Option<(Ipv4Addr, u16)> {
    let mut parts = line.split_whitespace();
    let port_token = parts.nth(3)?;
    let port: u16 = port_token.split('/').next()?.parse().ok()?;
    let address = parts.nth(1).and_then(|a| Ipv4Addr::from_str(a).ok())?;
    Some((address, port))
}

pub struct MasscanOptions {
    pub bin: String,
    pub config_path: String,
    pub use_sudo: bool,
}

pub async fn run_masscan<F, Fut>(opts: &MasscanOptions, mut on_hit: F) -> Result<()>
where
    F: FnMut(Ipv4Addr, u16) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let config = Path::new(&opts.config_path);
    if !config.is_file() {
        bail!(
            "masscan config not found at {} (set SEEKER_MASSCAN_CONFIG or --masscan-config)",
            config.display()
        );
    }

    let mut command = if opts.use_sudo {
        let mut cmd = Command::new("sudo");
        cmd.args(["masscan", "-c", &opts.config_path]);
        cmd
    } else {
        let mut cmd = Command::new(&opts.bin);
        cmd.args(["-c", &opts.config_path]);
        cmd
    };

    command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit());

    let mut child = command
        .spawn()
        .with_context(|| format!("failed to spawn masscan ({})", opts.bin))?;

    let stdout = child
        .stdout
        .take()
        .context("masscan produced no stdout")?;

    let mut reader = BufReader::new(stdout).lines();
    while let Ok(Some(line)) = reader.next_line().await {
        if let Some((address, port)) = parse_masscan_line(&line) {
            on_hit(address, port).await?;
        }
    }

    let status = child.wait().await.context("masscan wait")?;
    if !status.success() {
        bail!("masscan exited with {status}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_standard_masscan_line() {
        let (ip, port) = parse_masscan_line("Discovered open port 25565/tcp on 203.0.113.42").unwrap();
        assert_eq!(ip, Ipv4Addr::new(203, 0, 113, 42));
        assert_eq!(port, 25565);
    }

    #[test]
    fn ignores_non_discovery_lines() {
        assert!(parse_masscan_line("Starting masscan").is_none());
    }
}
