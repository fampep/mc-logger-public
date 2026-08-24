use anyhow::{Context, Result};
use ipnet::IpNet;
use std::net::IpAddr;
use std::str::FromStr;

pub fn parse_cidr(input: &str) -> Result<IpNet> {
    let trimmed = input.trim();
    if trimmed.contains('/') {
        return IpNet::from_str(trimmed).with_context(|| format!("invalid CIDR {input}"));
    }

    let addr = IpAddr::from_str(trimmed).with_context(|| format!("invalid IP {input}"))?;
    Ok(match addr {
        IpAddr::V4(v4) => IpNet::V4(format!("{v4}/32").parse()?),
        IpAddr::V6(v6) => IpNet::V6(format!("{v6}/128").parse()?),
    })
}

pub fn expand_cidr(net: &IpNet, limit: Option<usize>) -> Result<Vec<IpAddr>> {
    let mut addrs = Vec::new();
    for addr in net.hosts() {
        addrs.push(addr);
        if let Some(max) = limit {
            if addrs.len() >= max {
                break;
            }
        }
    }
    Ok(addrs)
}

pub fn display_ip(addr: IpAddr) -> String {
    match addr {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => format!("[{v6}]"),
    }
}

pub fn is_private(addr: IpAddr) -> bool {
    match addr {
        IpAddr::V4(v4) => v4.is_private() || v4.is_loopback() || v4.is_link_local(),
        IpAddr::V6(v6) => v6.is_loopback() || v6.is_unique_local(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_ip_becomes_slash_32() {
        let net = parse_cidr("10.0.0.5").unwrap();
        assert_eq!(net.to_string(), "10.0.0.5/32");
    }

    #[test]
    fn slash_24_has_254_hosts() {
        use std::net::Ipv4Addr;
        let net = parse_cidr("192.168.1.0/24").unwrap();
        let hosts = expand_cidr(&net, None).unwrap();
        assert_eq!(hosts.len(), 254);
        assert_eq!(hosts[0], IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)));
    }
}
