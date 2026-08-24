//! Text, color, table, and status-embed formatting.

use poise::serenity_prelude::{CreateEmbed, CreateEmbedFooter};

pub const PALETTE: Palette = Palette {
    brand: 0x5865f2,
    join: 0x57f287,
    leave: 0xed4245,
    death: 0x992d22,
    advancement: 0xfee75c,
    whisper: 0xeb459e,
    neutral: 0x9aa0a6,
    muted: 0x2b2d31,
};

pub struct Palette {
    pub brand: u32,
    pub join: u32,
    pub leave: u32,
    pub death: u32,
    pub advancement: u32,
    pub whisper: u32,
    pub neutral: u32,
    pub muted: u32,
}

pub fn kind_color(kind: &str) -> u32 {
    match kind {
        "chat" | "c" => PALETTE.brand,
        "whisper" | "w" => PALETTE.whisper,
        "join" | "j" => PALETTE.join,
        "leave" | "l" => PALETTE.leave,
        "death" | "d" | "kill" => PALETTE.death,
        "advancement" | "a" => PALETTE.advancement,
        "server" | "s" => PALETTE.neutral,
        _ => PALETTE.brand,
    }
}

/// One accent per command category so the colour tells you what you are looking at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Category {
    Stats,
    Server,
    Admin,
}

impl Category {
    pub fn color(self) -> u32 {
        match self {
            Self::Stats => 0x5865f2,
            Self::Server => 0x1abc9c,
            Self::Admin => 0x99aab5,
        }
    }
}

pub fn skeleton(category: Category) -> CreateEmbed {
    CreateEmbed::new()
        .colour(category.color())
        .timestamp(chrono::Utc::now())
}

pub fn with_context(embed: CreateEmbed, server_label: &str, extra: &[&str]) -> CreateEmbed {
    embed.footer(CreateEmbedFooter::new(footer(server_label, extra)))
}

/// Compact numbers for humans: 1247 → 1.2k, 400 stays 400.
pub fn compact(n: i64) -> String {
    let sign = if n < 0 { "-" } else { "" };
    let a = n.unsigned_abs();
    if a >= 1_000_000 {
        format!("{sign}{:.1}M", a as f64 / 1_000_000.0)
    } else if a >= 1_000 {
        format!("{sign}{:.1}k", a as f64 / 1_000.0)
    } else {
        format!("{sign}{a}")
    }
}

/// One overview row matching player `/stats`: `` `Label:` **value** ``.
pub fn overview_line(label: &str, value: impl AsRef<str>) -> String {
    format!("`{}:` {}", label, value.as_ref())
}

pub fn overview_num(label: &str, n: i64) -> String {
    overview_line(label, format!("**{}**", fmt(n as f64)))
}

pub fn yes_no(yes: bool) -> &'static str {
    if yes {
        "✅ **Yes**"
    } else {
        "❌ **No**"
    }
}

pub fn trend_i64(current: i64, previous: i64) -> String {
    let delta = current - previous;
    if delta > 0 {
        format!("▲ {}", compact(delta))
    } else if delta < 0 {
        format!("▼ {}", compact(-delta))
    } else {
        "• same".into()
    }
}

/// Rainbow mode: a colour that sweeps the full hue wheel once a minute,
/// driven by the event's own timestamp rather than a shared counter — so it's
/// deterministic (same event always renders the same colour, even after a
/// restart) and needs no mutable state threaded through the bridge.
pub fn rainbow_color(ts: chrono::DateTime<chrono::Utc>) -> u32 {
    const DEGREES_PER_SEC: f64 = 6.0; // 360° / 60s
    let hue = (ts.timestamp_millis() as f64 / 1000.0 * DEGREES_PER_SEC).rem_euclid(360.0);
    hsl_to_int(hue, 0.75, 0.6)
}

fn hsl_to_int(h: f64, s: f64, l: f64) -> u32 {
    let c = (1.0 - (2.0 * l - 1.0).abs()) * s;
    let x = c * (1.0 - ((h / 60.0) % 2.0 - 1.0).abs());
    let m = l - c / 2.0;
    let (r, g, b) = if h < 60.0 {
        (c, x, 0.0)
    } else if h < 120.0 {
        (x, c, 0.0)
    } else if h < 180.0 {
        (0.0, c, x)
    } else if h < 240.0 {
        (0.0, x, c)
    } else if h < 300.0 {
        (x, 0.0, c)
    } else {
        (c, 0.0, x)
    };
    let r = ((r + m) * 255.0).round() as u32;
    let g = ((g + m) * 255.0).round() as u32;
    let b = ((b + m) * 255.0).round() as u32;
    (r << 16) | (g << 8) | b
}

pub struct Limits;
impl Limits {
    pub const DESCRIPTION: usize = 4096;
    pub const FIELD: usize = 1024;
    pub const CONTENT: usize = 1900;
    pub const TOPIC: usize = 1024;
}

pub fn clamp(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let truncated: String = text.chars().take(max.saturating_sub(1)).collect();
    format!("{truncated}…")
}

pub fn join_lines(lines: &[String], max: usize) -> String {
    let mut kept = Vec::new();
    let mut length = 0usize;
    for line in lines {
        if length + line.len() + 1 > max {
            kept.push(format!("…and {} more", lines.len() - kept.len()));
            break;
        }
        kept.push(line.clone());
        length += line.len() + 1;
    }
    kept.join("\n")
}

pub fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

const MAX_LINE_CHARS: usize = 700;
const MAX_NAME_CHARS: usize = 16;

pub fn log_text(text: &str) -> String {
    clamp(&escape_md(text), MAX_LINE_CHARS)
}

pub fn escape_md(text: &str) -> String {
    let mut out = String::new();
    for ch in one_line(text).chars() {
        if matches!(ch, '\\' | '*' | '_' | '~' | '`' | '|') {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

pub fn code(text: impl ToString) -> String {
    format!("`{}`", text.to_string().replace('`', "ˋ"))
}

pub fn code_block(text: &str) -> String {
    format!("```\n{text}\n```")
}

pub fn fenced(lines: &[String], max: usize) -> String {
    let mut kept = Vec::new();
    let mut length = 8usize;
    for line in lines {
        if length + line.len() + 1 > max {
            break;
        }
        kept.push(line.clone());
        length += line.len() + 1;
    }
    code_block(&kept.join("\n"))
}

pub fn bold(text: &str) -> String {
    format!("**{text}**")
}

pub fn italic(text: &str) -> String {
    format!("*{text}*")
}

pub fn player_name(name: Option<&str>) -> String {
    bold(&clamp(&escape_md(name.unwrap_or("?")), MAX_NAME_CHARS))
}

pub fn short_time(ts: chrono::DateTime<chrono::Utc>) -> String {
    format!("<t:{}:t>", ts.timestamp())
}

/// Date and time, rendered by Discord in each viewer's own timezone and clock
/// format — so the same message reads 4:20 PM to one person and 16:20 to
/// another, and neither has to work out the offset from UTC.
///
/// Only ever use these outside code fences: Discord does not expand `<t:…>`
/// inside a fenced block, it prints the raw tag.
pub fn date_time(ts: chrono::DateTime<chrono::Utc>) -> String {
    format!("<t:{}:f>", ts.timestamp())
}

/// Just the day, no clock. For "joined on" style lines where a time is noise.
pub fn date_only(ts: chrono::DateTime<chrono::Utc>) -> String {
    format!("<t:{}:D>", ts.timestamp())
}

pub fn fmt(value: impl Into<f64>) -> String {
    let n = value.into().round() as i64;
    let s = n.abs().to_string();
    let mut out = String::new();
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    let digits: String = out.chars().rev().collect();
    if n < 0 {
        format!("-{digits}")
    } else {
        digits
    }
}

/// Seconds of playtime as `12d 4h 30m`, dropping leading zero units. Always
/// shows minutes, even for a session under a minute (`0m`), so the field
/// never renders blank.
pub fn duration_hm(total_secs: f64) -> String {
    let total_mins = (total_secs.max(0.0) / 60.0).round() as i64;
    let days = total_mins / (24 * 60);
    let hours = (total_mins / 60) % 24;
    let mins = total_mins % 60;
    let mut parts = Vec::new();
    if days > 0 {
        parts.push(format!("{days}d"));
    }
    if hours > 0 || days > 0 {
        parts.push(format!("{hours}h"));
    }
    parts.push(format!("{mins}m"));
    parts.join(" ")
}

pub fn ratio(top: i64, bottom: i64) -> String {
    if bottom == 0 {
        return if top == 0 {
            "0.00".into()
        } else {
            format!("{top}.00")
        };
    }
    format!("{:.2}", top as f64 / bottom as f64)
}

#[derive(Clone)]
pub struct Column {
    pub header: String,
    pub align_right: bool,
    pub width: Option<usize>,
}

impl Column {
    pub fn new(header: impl Into<String>) -> Self {
        Self {
            header: header.into(),
            align_right: false,
            width: None,
        }
    }

    pub fn right(mut self) -> Self {
        self.align_right = true;
        self
    }

    pub fn width(mut self, w: usize) -> Self {
        self.width = Some(w);
        self
    }
}

pub fn table(columns: &[Column], rows: &[Vec<String>], max: usize) -> String {
    let widths: Vec<usize> = columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let longest = rows
                .iter()
                .map(|r| r.get(i).map(|s| s.chars().count()).unwrap_or(0))
                .max()
                .unwrap_or(0)
                .max(col.header.chars().count());
            col.width.map(|w| w.min(longest)).unwrap_or(longest)
        })
        .collect();

    let render = |values: &[String]| -> String {
        values
            .iter()
            .enumerate()
            .map(|(i, value)| {
                let width = widths.get(i).copied().unwrap_or(value.chars().count());
                let cell = if value.chars().count() > width {
                    clamp(value, width)
                } else {
                    value.clone()
                };
                let pad = width.saturating_sub(cell.chars().count());
                if columns.get(i).map(|c| c.align_right).unwrap_or(false) {
                    format!("{}{cell}", " ".repeat(pad))
                } else {
                    format!("{cell}{}", " ".repeat(pad))
                }
            })
            .collect::<Vec<_>>()
            .join("  ")
            .trim_end()
            .to_string()
    };

    let header = render(
        &columns
            .iter()
            .map(|c| c.header.to_uppercase())
            .collect::<Vec<_>>(),
    );
    let mut lines = vec![header];
    lines.extend(rows.iter().map(|r| render(r)));
    fenced(&lines, max)
}

pub fn footer(server_label: &str, extra: &[&str]) -> String {
    let mut parts = vec![server_label.to_string()];
    for e in extra {
        if !e.is_empty() {
            parts.push((*e).to_string());
        }
    }
    parts.join("  ·  ")
}

pub fn notice(title: &str, body: &str) -> CreateEmbed {
    skeleton(Category::Server).title(title).description(body)
}

pub fn warning(title: &str, body: &str) -> CreateEmbed {
    CreateEmbed::new()
        .colour(0xfaa61a)
        .title(title)
        .description(body)
        .timestamp(chrono::Utc::now())
}

pub fn failure(title: &str, body: &str) -> CreateEmbed {
    CreateEmbed::new()
        .colour(PALETTE.leave)
        .title(title)
        .description(body)
        .timestamp(chrono::Utc::now())
}

pub fn not_found(title: &str, body: &str) -> CreateEmbed {
    warning(title, body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_hm_drops_leading_zero_units() {
        assert_eq!(duration_hm(0.0), "0m");
        assert_eq!(duration_hm(20.0), "0m");
        assert_eq!(duration_hm(90.0), "2m");
        assert_eq!(duration_hm(3600.0), "1h 0m");
        assert_eq!(duration_hm(3660.0), "1h 1m");
        assert_eq!(duration_hm(90_000.0), "1d 1h 0m");
    }
}
