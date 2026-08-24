use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// Minecraft Java status ping (protocol 1.7+).
/// Sequence: Handshake (next state = status) ??? Status Request ??? Status Response ??? Ping/Pong.
const DEFAULT_PROTOCOL_VERSION: i32 = 767; // 1.21

pub struct PingResult {
    pub motd: String,
    pub version_name: String,
    pub protocol: i32,
    pub players_online: u32,
    pub players_max: u32,
    pub raw_json: String,
}

#[derive(Debug, Deserialize)]
struct StatusJson {
    description: Option<serde_json::Value>,
    version: Option<VersionJson>,
    players: Option<PlayersJson>,
}

#[derive(Debug, Deserialize)]
struct VersionJson {
    name: Option<String>,
    protocol: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct PlayersJson {
    online: Option<u32>,
    max: Option<u32>,
}

pub async fn status_ping(host: &str, port: u16, connect_timeout: Duration) -> Result<PingResult> {
    let addr = format!("{host}:{port}");
    let mut stream = timeout(connect_timeout, TcpStream::connect(&addr))
        .await
        .context("connect timed out")?
        .with_context(|| format!("failed to connect to {addr}"))?;

    write_packet(
        &mut stream,
        0x00,
        &build_handshake(host, port, DEFAULT_PROTOCOL_VERSION, 1),
    )
    .await?;
    write_packet(&mut stream, 0x00, &[]).await?;

    let json = read_status_json(&mut stream, connect_timeout).await?;
    stream.shutdown().await.ok();

    let parsed: StatusJson = serde_json::from_str(&json).context("invalid status JSON")?;
    let motd = flatten_motd(parsed.description.as_ref());
    let version_name = parsed
        .version
        .as_ref()
        .and_then(|v| v.name.clone())
        .unwrap_or_default();
    let protocol = parsed
        .version
        .as_ref()
        .and_then(|v| v.protocol)
        .unwrap_or(0);
    let players_online = parsed
        .players
        .as_ref()
        .and_then(|p| p.online)
        .unwrap_or(0);
    let players_max = parsed
        .players
        .as_ref()
        .and_then(|p| p.max)
        .unwrap_or(0);

    Ok(PingResult {
        motd,
        version_name,
        protocol,
        players_online,
        players_max,
        raw_json: json,
    })
}



fn try_parse_status_buffer(buf: &[u8]) -> Result<String> {
    let (packet_len, body_off) = read_var_int(buf, 0)?;
    if packet_len < 0 {
        bail!("negative packet length");
    }
    let body_end = body_off + packet_len as usize;
    if buf.len() < body_end {
        bail!("incomplete packet");
    }
    let body = &buf[body_off..body_end];
    let (packet_id, str_off) = read_var_int(body, 0)?;
    if packet_id != 0x00 {
        bail!("unexpected status response packet id {packet_id}");
    }
    read_string(body, str_off)
}

async fn read_status_json(stream: &mut TcpStream, read_timeout: Duration) -> Result<String> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = timeout(read_timeout, stream.read(&mut chunk))
            .await
            .context("read timed out")??;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Ok(json) = try_parse_status_buffer(&buf) {
            return Ok(json);
        }
        if buf.len() > 1 << 20 {
            bail!("status response too large");
        }
    }
    try_parse_status_buffer(&buf).context("incomplete status response")
}
fn build_handshake(host: &str, port: u16, protocol: i32, next_state: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, protocol);
    write_string(&mut payload, host);
    payload.extend_from_slice(&port.to_be_bytes());
    write_var_int(&mut payload, next_state);
    payload
}

async fn write_packet(stream: &mut TcpStream, packet_id: i32, payload: &[u8]) -> Result<()> {
    let mut packet = Vec::new();
    write_var_int(&mut packet, packet_id);
    packet.extend_from_slice(payload);
    let mut framed = Vec::new();
    write_var_int(&mut framed, packet.len() as i32);
    framed.extend_from_slice(&packet);
    stream.write_all(&framed).await?;
    Ok(())
}

async fn read_packet(stream: &mut TcpStream, read_timeout: Duration) -> Result<Vec<u8>> {
    let mut length_buf = [0u8; 1];
    let mut packet = Vec::new();
    let mut length_bytes = 0;
    let length = loop {
        timeout(read_timeout, stream.read_exact(&mut length_buf))
            .await
            .context("read timed out")??;
        packet.push(length_buf[0]);
        length_bytes += 1;
        let (value, _) = read_var_int(&packet, 0)?;
        if length_buf[0] & 0x80 == 0 {
            break value;
        }
        if length_bytes > 5 {
            bail!("invalid packet length varint");
        }
    };

    let remaining = length as usize;
    if remaining > 0 {
        let mut rest = vec![0u8; remaining];
        timeout(read_timeout, stream.read_exact(&mut rest))
            .await
            .context("read timed out")??;
        packet.extend_from_slice(&rest);
    }

    Ok(packet[length_bytes..].to_vec())
}

fn read_var_int(buf: &[u8], mut offset: usize) -> Result<(i32, usize)> {
    let mut value = 0i32;
    let mut shift = 0;
    loop {
        if offset >= buf.len() {
            bail!("unexpected end of buffer reading varint");
        }
        let byte = buf[offset];
        offset += 1;
        value |= (byte as i32 & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, offset));
        }
        shift += 7;
        if shift >= 32 {
            bail!("varint too large");
        }
    }
}

fn write_var_int(buf: &mut Vec<u8>, mut value: i32) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn read_string(buf: &[u8], offset: usize) -> Result<String> {
    let (len, offset) = read_var_int(buf, offset)?;
    if len < 0 {
        bail!("negative string length");
    }
    let end = offset + len as usize;
    if end > buf.len() {
        bail!("string extends past buffer");
    }
    Ok(String::from_utf8_lossy(&buf[offset..end]).into_owned())
}

fn write_string(buf: &mut Vec<u8>, value: &str) {
    write_var_int(buf, value.len() as i32);
    buf.extend_from_slice(value.as_bytes());
}

fn flatten_motd(value: Option<&serde_json::Value>) -> String {
    match value {
        None => String::new(),
        Some(serde_json::Value::String(s)) => strip_formatting(s),
        Some(v) => strip_formatting(&flatten_json_text(v)),
    }
}

fn flatten_json_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(items) => items.iter().map(flatten_json_text).collect(),
        serde_json::Value::Object(map) => {
            let mut out = String::new();
            if let Some(serde_json::Value::String(text)) = map.get("text") {
                out.push_str(text);
            }
            if let Some(extra) = map.get("extra") {
                out.push_str(&flatten_json_text(extra));
            }
            out
        }
        _ => String::new(),
    }
}

fn strip_formatting(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{a7}' {
            chars.next();
            continue;
        }
        out.push(ch);
    }
    out.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flatten_motd_from_json_object() {
        let json: serde_json::Value = serde_json::json!({
            "text": "Hello ",
            "extra": [{"text": "World", "color": "green"}]
        });
        assert_eq!(flatten_motd(Some(&json)), "Hello World");
    }
}

