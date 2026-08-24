use poise::serenity_prelude::AutocompleteChoice;

use crate::config::ServerConfig;
use crate::presentation::ui::{not_found, send_embed};
use crate::{Context, Error};

pub const PLAYER_REF_SEP: &str = "::";
pub const MAX_PAGES: usize = 25;
pub const EVENTS_PER_SCREEN: usize = 50;
pub const EVENTS_PER_TAB: i64 = 200;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, poise::ChoiceParameter)]
pub enum RangeChoice {
    #[name = "7d"]
    D7,
    #[name = "30d"]
    #[default]
    D30,
    #[name = "60d"]
    D60,
    #[name = "90d"]
    D90,
    #[name = "all"]
    All,
}

impl RangeChoice {
    pub fn days(self) -> Option<i32> {
        match self {
            Self::D7 => Some(7),
            Self::D30 => Some(30),
            Self::D60 => Some(60),
            Self::D90 => Some(90),
            Self::All => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::D7 => "last 7 days",
            Self::D30 => "last 30 days",
            Self::D60 => "last 60 days",
            Self::D90 => "last 90 days",
            Self::All => "all time",
        }
    }

    pub fn as_index(self) -> usize {
        match self {
            Self::D7 => 0,
            Self::D30 => 1,
            Self::D60 => 2,
            Self::D90 => 3,
            Self::All => 4,
        }
    }

    pub fn from_index(index: usize) -> Self {
        match index {
            1 => Self::D30,
            2 => Self::D60,
            3 => Self::D90,
            4 => Self::All,
            _ => Self::D7,
        }
    }

    /// Previous window of the same length: (since_days, until_days).
    pub fn previous(self) -> Option<(i32, i32)> {
        self.days().map(|d| (d * 2, d))
    }
}

/// A dropdown of valid boards, so a typo can't silently fall back to kills —
/// `board` used to be a free-text string with no autocomplete or validation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, poise::ChoiceParameter)]
pub enum BoardChoice {
    #[name = "PvP kills"]
    #[default]
    Kills,
    #[name = "K/D"]
    Kd,
    #[name = "Deaths"]
    Deaths,
    #[name = "Messages"]
    Messages,
    #[name = "Joins"]
    Joins,
    #[name = "Playtime"]
    Playtime,
}

impl BoardChoice {
    /// Index into `BOARD_MENU` / the `metrics` array used by `/leaderboard`.
    pub fn as_index(self) -> usize {
        match self {
            Self::Kills => 0,
            Self::Kd => 1,
            Self::Deaths => 2,
            Self::Messages => 3,
            Self::Joins => 4,
            Self::Playtime => 5,
        }
    }
}

pub fn encode_player_ref(server_key: &str, name: &str) -> String {
    format!("{server_key}{PLAYER_REF_SEP}{name}")
}

pub fn parse_player_ref(raw: Option<&str>) -> Option<(Option<String>, String)> {
    let trimmed = raw?.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some((server, name)) = trimmed.split_once(PLAYER_REF_SEP) {
        if !server.is_empty() && !name.is_empty() {
            return Some((Some(server.to_string()), name.to_string()));
        }
    }
    Some((None, trimmed.to_string()))
}

pub fn resolve_server(
    ctx: Context<'_>,
    server_opt: Option<&str>,
    player_opt: Option<&str>,
) -> ServerConfig {
    let config = &ctx.data().config;
    if let Some(key) = server_opt {
        if let Some(s) = config.server_by_key(key) {
            return s.clone();
        }
    }
    if let Some((Some(key), _)) = parse_player_ref(player_opt) {
        if let Some(s) = config.server_by_key(&key) {
            return s.clone();
        }
    }
    config.servers[0].clone()
}

/// Same as [`resolve_server`], but a `server` option that matches no
/// configured key stops the command with a clear reply instead of silently
/// falling back to the first server — a typo'd key used to just show data for
/// the wrong server with no indication anything was off.
///
/// `Ok(None)` means the "unknown server" reply was already sent; the caller
/// should return early.
pub async fn resolve_server_checked(
    ctx: Context<'_>,
    server_opt: Option<&str>,
    player_opt: Option<&str>,
) -> Result<Option<ServerConfig>, Error> {
    if let Some(key) = server_opt {
        let config = &ctx.data().config;
        if config.server_by_key(key).is_none() {
            let known = config
                .servers
                .iter()
                .map(|s| s.key.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            send_embed(
                ctx,
                not_found(
                    "Unknown server",
                    &format!("\"{key}\" isn't a configured server. Known: {known}."),
                ),
            )
            .await?;
            return Ok(None);
        }
    }
    Ok(Some(resolve_server(ctx, server_opt, player_opt)))
}

pub async fn autocomplete_player(ctx: Context<'_>, partial: &str) -> Vec<AutocompleteChoice> {
    let config = &ctx.data().config;
    let multi = config.multi_server();
    // Slash autocomplete does not expose sibling option values reliably via
    // poise helpers; search across all servers (same as an unset server filter).
    let hits = ctx
        .data()
        .db
        .search_players_across_servers(partial.trim(), 25, None)
        .await;
    hits.into_iter()
        .map(|hit| {
            let label = if multi {
                format!("{} · {}", hit.name, hit.server_label)
            } else {
                hit.name.clone()
            };
            let label = if label.chars().count() > 100 {
                crate::presentation::ui::clamp(&label, 100)
            } else {
                label
            };
            AutocompleteChoice::new(label, encode_player_ref(&hit.server_key, &hit.name))
        })
        .collect()
}

pub async fn autocomplete_server(ctx: Context<'_>, partial: &str) -> Vec<AutocompleteChoice> {
    let partial = partial.to_lowercase();
    ctx.data()
        .config
        .servers
        .iter()
        .filter(|s| {
            partial.is_empty()
                || s.key.contains(&partial)
                || s.label.to_lowercase().contains(&partial)
        })
        .take(25)
        .map(|s| AutocompleteChoice::new(s.label.clone(), s.key.clone()))
        .collect()
}

pub fn page_count_for(total: i64, per_page: usize) -> usize {
    let pages = ((total as f64) / (per_page as f64)).ceil() as usize;
    pages.clamp(1, MAX_PAGES)
}

pub fn player_name_arg(raw: Option<&str>) -> Option<String> {
    parse_player_ref(raw).map(|(_, name)| name)
}

#[allow(dead_code)]
pub type CmdResult = Result<(), Error>;
