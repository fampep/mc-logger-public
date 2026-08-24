//! Data types shared by database queries, commands, and Discord rendering.

use chrono::{DateTime, Utc};

use crate::config::BridgeStyle;

pub const MIN_KILLS_FOR_KD: i64 = 5;

#[derive(Debug, Clone)]
pub struct ChatRow {
    pub id: i64,
    pub received_at: DateTime<Utc>,
    pub kind: String,
    pub sender_name: Option<String>,
    pub sender_label: Option<String>,
    pub subject_name: Option<String>,
    pub killer_name: Option<String>,
    /// Chat/whisper: message body. Advancement: `[title]` or a historical full
    /// line. Death/join/leave: empty — Discord rebuilds the display string.
    pub plain_text: String,
    pub server_host: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BridgeSettings {
    pub channel_id: Option<String>,
    pub enabled: bool,
    pub style: BridgeStyle,
    pub rainbow: bool,
}

#[derive(Debug, Clone)]
pub struct EventRoute {
    pub kind: String,
    pub enabled: bool,
    pub channel_id: Option<String>,
}

/// Kinds that `/chatbridge customize` can route. Packed DB codes map onto these names.
pub const ROUTE_KINDS: &[&str] = &[
    "chat",
    "join",
    "leave",
    "death",
    "advancement",
    "server",
    "whisper",
];

pub fn route_kind_label(kind: &str) -> &'static str {
    match kind {
        "chat" => "Chat",
        "join" => "Joins",
        "leave" => "Leaves",
        "death" => "Deaths",
        "advancement" => "Advancements",
        "server" => "Server",
        "whisper" => "Whispers",
        _ => "Event",
    }
}

pub fn canonical_route_kind(kind: &str) -> Option<&'static str> {
    match kind.trim().to_ascii_lowercase().chars().next()? {
        'c' => Some("chat"),
        'w' => Some("whisper"),
        'j' => Some("join"),
        'l' => Some("leave"),
        'd' => Some("death"),
        'a' => Some("advancement"),
        's' => Some("server"),
        _ => None,
    }
}

/// Join/leave codes in `player_events` (snapshots `'s'` are not feed lines).
pub fn presence_kind(event_type: &str) -> Option<&'static str> {
    match event_type.trim().chars().next()?.to_ascii_lowercase() {
        'j' => Some("join"),
        'l' => Some("leave"),
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub struct WatchHit {
    pub id: i64,
    pub occurred_at: DateTime<Utc>,
    pub event_type: String,
    pub player_name: String,
    pub server_host: Option<String>,
    pub user_id: String,
    pub channel_id: String,
}

#[derive(Debug, Clone)]
pub struct OverallStats {
    pub servers: i64,
    pub sessions: i64,
    pub messages: i64,
    pub chat: i64,
    pub players: i64,
    pub joins: i64,
    pub leaves: i64,
    pub deaths: i64,
    pub kills: i64,
    pub goals: i64,
    pub online: i64,
    pub first_seen: Option<DateTime<Utc>>,
    pub last_seen: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
pub struct WindowStats {
    pub chat: i64,
    pub joins: i64,
    pub leaves: i64,
    pub deaths: i64,
    pub kills: i64,
    pub goals: i64,
    pub players: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaderMetric {
    Kills,
    Deaths,
    Kd,
    Messages,
    Joins,
    Playtime,
}

impl LeaderMetric {
    pub fn parse(s: &str) -> Self {
        match s {
            "deaths" => Self::Deaths,
            "kd" => Self::Kd,
            "messages" => Self::Messages,
            "joins" => Self::Joins,
            "playtime" => Self::Playtime,
            _ => Self::Kills,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Kills => "kills",
            Self::Deaths => "deaths",
            Self::Kd => "kd",
            Self::Messages => "messages",
            Self::Joins => "joins",
            Self::Playtime => "playtime",
        }
    }
}

#[derive(Debug, Clone)]
pub struct LeaderRow {
    pub name: String,
    pub value: f64,
    pub kills: Option<i64>,
    pub deaths: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct PlayerStats {
    pub name: String,
    pub uuid: Option<String>,
    pub messages: i64,
    pub deaths: i64,
    pub joins: i64,
    pub leaves: i64,
    pub kills: i64,
    pub advancements: i64,
    pub first_seen: Option<DateTime<Utc>>,
    pub last_seen: Option<DateTime<Utc>>,
    pub last_message_at: Option<DateTime<Utc>>,
    pub kill_rank: Option<i64>,
    pub chat_rank: Option<String>,
    pub playtime_secs: f64,
    pub session_count: i64,
}

impl PlayerStats {
    pub fn is_quiet(&self) -> bool {
        self.messages == 0
            && self.deaths == 0
            && self.joins == 0
            && self.leaves == 0
            && self.kills == 0
            && self.advancements == 0
            && self.session_count == 0
            && self.playtime_secs < 1.0
    }
}

#[derive(Debug, Clone)]
pub struct RivalRow {
    pub name: String,
    pub count: i64,
}

#[derive(Debug, Clone)]
pub struct PlayerHit {
    pub name: String,
    pub server_key: String,
    pub server_label: String,
}

#[derive(Debug, Clone)]
pub struct ChatHistoryRow {
    pub received_at: DateTime<Utc>,
    pub sender_name: Option<String>,
    pub plain_text: String,
    pub server_host: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ChatQuery {
    pub player: Option<String>,
    pub search: Option<String>,
    pub limit: i64,
    pub offset: i64,
    pub since_days: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct TopicStats {
    pub host: Option<String>,
    pub online: i64,
    pub peak_today: i64,
    pub players_24h: i64,
    pub chat_1h: i64,
    pub joins_1h: i64,
    pub deaths_24h: i64,
    pub logger_live: bool,
    pub last_message_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct DatabaseStats {
    pub database: String,
    pub size: String,
    pub bytes: i64,
    pub oldest: Option<DateTime<Utc>>,
    pub newest: Option<DateTime<Utc>>,
    pub chat: i64,
    pub joins: i64,
    pub leaves: i64,
    pub deaths: i64,
    pub kills: i64,
    pub players: i64,
    pub sessions: i64,
    pub goals: i64,
}

#[derive(Debug, Clone)]
pub struct EventRow {
    pub occurred_at: DateTime<Utc>,
    pub player_name: String,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    Chat,
    Join,
    Leave,
    Death,
    Kill,
    Advancement,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::Join => "join",
            Self::Leave => "leave",
            Self::Death => "death",
            Self::Kill => "kill",
            Self::Advancement => "advancement",
        }
    }

    /// Compact DB codes plus legacy long names.
    pub fn db_codes(self) -> &'static [&'static str] {
        match self {
            Self::Chat => &["c", "chat"],
            Self::Join => &["j", "join"],
            Self::Leave => &["l", "leave"],
            Self::Death => &["d", "death"],
            Self::Advancement => &["a", "advancement"],
            Self::Kill => &[],
        }
    }

    /// First-letter codes for `BRIDGE_KINDS` (`chat` → `c`). Packed `"char"`
    /// columns store only this letter; long names never match `kind::text = ANY(...)`.
    pub fn filter_codes(kinds: &[String]) -> Vec<String> {
        let mut codes: Vec<String> = kinds
            .iter()
            .filter_map(|k| {
                let k = k.trim().to_ascii_lowercase();
                Some(match k.as_str() {
                    "chat" | "c" => "c".to_string(),
                    "whisper" | "w" => "w".to_string(),
                    "join" | "j" => "j".to_string(),
                    "leave" | "l" => "l".to_string(),
                    "death" | "d" => "d".to_string(),
                    "advancement" | "a" | "goal" | "goals" => "a".to_string(),
                    "server" | "s" => "s".to_string(),
                    "unknown" | "u" => "u".to_string(),
                    other => other.chars().next()?.to_string(),
                })
            })
            .collect();
        codes.sort();
        codes.dedup();
        codes
    }

    pub fn matches_name(configured: &[String], event_kind: &str) -> bool {
        let codes = Self::filter_codes(configured);
        let got = event_kind
            .trim()
            .chars()
            .next()
            .map(|c| c.to_ascii_lowercase().to_string())
            .unwrap_or_default();
        codes.iter().any(|c| c == &got)
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "join" | "j" => Self::Join,
            "leave" | "l" => Self::Leave,
            "death" | "d" => Self::Death,
            "kill" => Self::Kill,
            "advancement" | "a" => Self::Advancement,
            _ => Self::Chat,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_kinds_accept_compact_and_long_database_values() {
        assert_eq!(canonical_route_kind("c"), Some("chat"));
        assert_eq!(canonical_route_kind("whisper"), Some("whisper"));
        assert_eq!(canonical_route_kind(" J "), Some("join"));
        assert_eq!(canonical_route_kind("unknown"), None);
    }

    #[test]
    fn event_filters_are_normalized_and_deduplicated() {
        assert_eq!(
            EventKind::filter_codes(&[
                "chat".into(),
                "c".into(),
                "ADVANCEMENT".into(),
                "goals".into(),
            ]),
            vec!["a", "c"]
        );
    }

    #[test]
    fn presence_only_accepts_join_and_leave_events() {
        assert_eq!(presence_kind("join"), Some("join"));
        assert_eq!(presence_kind("l"), Some("leave"));
        assert_eq!(presence_kind("snapshot"), None);
    }
}
