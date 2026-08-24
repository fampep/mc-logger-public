use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Mutex;

use crate::commands::shared::{
    autocomplete_player, autocomplete_server, player_name_arg, resolve_server_checked, RangeChoice,
    EVENTS_PER_SCREEN, EVENTS_PER_TAB,
};
use crate::config::ServerConfig;
use crate::database::{ChatQuery, EventKind, EventRow, PlayerStats};
use crate::presentation::embeds::{
    build_events_page_embed, build_player_log_embed, build_player_overview_embed,
    build_stats_overview_embed, build_top_boards_embed, player_log_line,
};
use crate::presentation::ui::{
    not_found, send_choices, send_embed, send_nav_pager, send_paged, NavPage, PALETTE,
    TIMEFRAME_SELECT,
};
use crate::{Context, Error};

type EventCache = Arc<Mutex<std::collections::HashMap<(usize, usize), Vec<EventRow>>>>;

const PLAYER_PAGES: usize = 5;
const LOG_LIMIT: i64 = 50;
const SERVER_TABS: &[&str] = &[
    "Totals",
    "Top",
    "Chat",
    "Joins",
    "Leaves",
    "Deaths",
    "Kills",
    "Advancements",
];

#[poise::command(slash_command)]
/// Player or server stats for a time window
pub async fn stats(
    ctx: Context<'_>,
    #[description = "Player name — omit for the whole server"]
    #[autocomplete = "autocomplete_player"]
    player: Option<String>,
    #[description = "How far back"] range: Option<RangeChoice>,
    #[description = "Which server"]
    #[autocomplete = "autocomplete_server"]
    server: Option<String>,
) -> Result<(), Error> {
    let _ = ctx.defer().await;
    let window = range.unwrap_or_default();
    let Some(server_cfg) =
        resolve_server_checked(ctx, server.as_deref(), player.as_deref()).await?
    else {
        return Ok(());
    };
    let Some(name) = player_name_arg(player.as_deref()) else {
        return show_server_stats(ctx, server_cfg, window).await;
    };

    if server.is_none() && ctx.data().config.multi_server() {
        let hits = ctx.data().db.servers_with_player(&name).await;
        if hits.len() > 1 {
            let choices: Vec<_> = hits
                .iter()
                .map(|h| (h.server_key.clone(), h.server_label.clone()))
                .collect();
            let embed = crate::presentation::ui::notice(
                &format!("Which server for {name}?"),
                "That name appears on more than one logged server.",
            );
            return send_choices(ctx, embed, &choices, move |key| {
                let name = name.clone();
                async move {
                    let server = ctx
                        .data()
                        .config
                        .server_by_key(&key)
                        .cloned()
                        .unwrap_or_else(|| ctx.data().config.servers[0].clone());
                    show_player(ctx, server, &name, window).await
                }
            })
            .await;
        }
        if hits.len() == 1 {
            let server = ctx
                .data()
                .config
                .server_by_key(&hits[0].server_key)
                .cloned()
                .unwrap_or(server_cfg);
            return show_player(ctx, server, &name, window).await;
        }
    }

    show_player(ctx, server_cfg, &name, window).await
}

async fn show_server_stats(
    ctx: Context<'_>,
    server: ServerConfig,
    window: RangeChoice,
) -> Result<(), Error> {
    let db = ctx.data().db.clone();
    let key = server.key.clone();
    let label = server.label.clone();
    let cache: std::sync::Arc<tokio::sync::Mutex<std::collections::HashMap<usize, Vec<EventRow>>>> =
        std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
    let days = window.days();
    let window_label = window.label();
    let started = Instant::now();

    let kinds: [Option<EventKind>; 8] = [
        None,
        None,
        Some(EventKind::Chat),
        Some(EventKind::Join),
        Some(EventKind::Leave),
        Some(EventKind::Death),
        Some(EventKind::Kill),
        Some(EventKind::Advancement),
    ];

    send_paged(
        ctx,
        SERVER_TABS.len(),
        Some(SERVER_TABS),
        true,
        0,
        {
            let db = db.clone();
            let key = key.clone();
            let cache = cache.clone();
            move |page| {
                let db = db.clone();
                let key = key.clone();
                let cache = cache.clone();
                Box::pin(async move {
                    if page <= 1 {
                        return Ok(1);
                    }
                    let kind = kinds[page].unwrap();
                    let mut guard = cache.lock().await;
                    if let std::collections::hash_map::Entry::Vacant(entry) = guard.entry(page) {
                        let rows = db.recent_events(&key, kind, EVENTS_PER_TAB, days).await?;
                        entry.insert(rows);
                    }
                    let len = guard.get(&page).map(|r| r.len()).unwrap_or(0);
                    Ok(((len as f64) / (EVENTS_PER_SCREEN as f64)).ceil().max(1.0) as usize)
                })
            }
        },
        {
            let cache = cache.clone();
            move || {
                if let Ok(mut g) = cache.try_lock() {
                    g.clear();
                }
            }
        },
        move |page, screen| {
            let db = db.clone();
            let key = key.clone();
            let label = label.clone();
            let cache = cache.clone();
            async move {
                if page == 1 {
                    let top_chatters = db
                        .leaderboard(&key, crate::database::LeaderMetric::Messages, 8, 0)
                        .await?;
                    let top_killers = db
                        .leaderboard(&key, crate::database::LeaderMetric::Kills, 8, 0)
                        .await?;
                    let top_deaths = db
                        .leaderboard(&key, crate::database::LeaderMetric::Deaths, 8, 0)
                        .await?;
                    let top_kd = db
                        .leaderboard(&key, crate::database::LeaderMetric::Kd, 8, 0)
                        .await?;
                    return Ok(build_top_boards_embed(
                        &label,
                        &top_chatters,
                        &top_killers,
                        &top_deaths,
                        &top_kd,
                    ));
                }
                if page >= 2 {
                    let kind = kinds[page].unwrap();
                    let mut guard = cache.lock().await;
                    if let std::collections::hash_map::Entry::Vacant(entry) = guard.entry(page) {
                        let rows = db.recent_events(&key, kind, EVENTS_PER_TAB, days).await?;
                        entry.insert(rows);
                    }
                    let rows = guard.get(&page).cloned().unwrap_or_default();
                    let from = screen * EVENTS_PER_SCREEN;
                    let slice = rows
                        .get(from..from.saturating_add(EVENTS_PER_SCREEN).min(rows.len()))
                        .unwrap_or(&[])
                        .to_vec();
                    let screens = ((rows.len() as f64) / (EVENTS_PER_SCREEN as f64))
                        .ceil()
                        .max(1.0) as usize;
                    return Ok(build_events_page_embed(
                        kind,
                        &slice,
                        &label,
                        window_label,
                        screen,
                        screens,
                        started.elapsed(),
                        None,
                        None,
                    ));
                }
                let current = db.window_stats(&key, days, None).await?;
                let previous = match window.previous() {
                    Some((since, until)) => {
                        Some(db.window_stats(&key, Some(since), Some(until)).await?)
                    }
                    None => None,
                };
                let overall = db.overall_stats(&key).await?;
                Ok(build_stats_overview_embed(
                    &label,
                    window_label,
                    &current,
                    previous.as_ref(),
                    overall.online,
                    overall.last_seen,
                ))
            }
        },
    )
    .await
}

async fn show_player(
    ctx: Context<'_>,
    server: ServerConfig,
    name: &str,
    window: RangeChoice,
) -> Result<(), Error> {
    let started = Instant::now();
    let lifetime = ctx.data().db.player_stats(&server.key, name).await?;
    let Some(lifetime) = lifetime else {
        let suggestions = ctx
            .data()
            .db
            .search_players_across_servers(name, 5, Some(&server.key))
            .await;
        let hint = if suggestions.is_empty() {
            "No player by that name.".into()
        } else {
            format!(
                "No exact match. Did you mean {}?",
                suggestions
                    .iter()
                    .map(|h| format!("`{}`", h.name))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        return send_embed(ctx, not_found("Player not found", &hint)).await;
    };

    let db = ctx.data().db.clone();
    let config = ctx.data().config.clone();
    let key = server.key.clone();
    let label = server.label.clone();
    let display = lifetime.name.clone();
    let count_cache = Arc::new(Mutex::new({
        let mut map = std::collections::HashMap::new();
        map.insert(RangeChoice::All.as_index() as u8, lifetime.clone());
        map
    }));
    let event_cache: EventCache = Arc::new(Mutex::new(std::collections::HashMap::new()));
    let lifetime = lifetime.clone();

    send_nav_pager(ctx, 0, Some(&TIMEFRAME_SELECT), window.as_index(), {
        let db = db.clone();
        let config = config.clone();
        let key = key.clone();
        let label = label.clone();
        let display = display.clone();
        let lifetime = lifetime.clone();
        let count_cache = count_cache.clone();
        let event_cache = event_cache.clone();
        move |page, range_idx| {
            let db = db.clone();
            let config = config.clone();
            let key = key.clone();
            let label = label.clone();
            let display = display.clone();
            let lifetime = lifetime.clone();
            let count_cache = count_cache.clone();
            let event_cache = event_cache.clone();
            async move {
                let mut count_cache = count_cache.lock().await;
                let mut event_cache = event_cache.lock().await;
                let embed = render_player_page(
                    &db,
                    &config,
                    &key,
                    &label,
                    &lifetime,
                    &display,
                    &mut count_cache,
                    &mut event_cache,
                    page,
                    range_idx,
                    started.elapsed(),
                )
                .await?;
                Ok(NavPage {
                    embed,
                    page_count: PLAYER_PAGES,
                })
            }
        }
    })
    .await
}

#[allow(clippy::too_many_arguments)] // One coordinator for the four player-stat page variants.
async fn render_player_page(
    db: &crate::database::Db,
    config: &crate::config::Config,
    key: &str,
    label: &str,
    lifetime: &PlayerStats,
    display: &str,
    count_cache: &mut std::collections::HashMap<u8, PlayerStats>,
    event_cache: &mut std::collections::HashMap<(usize, usize), Vec<EventRow>>,
    page: usize,
    range_idx: usize,
    elapsed: std::time::Duration,
) -> Result<poise::serenity_prelude::CreateEmbed, Error> {
    let window = RangeChoice::from_index(range_idx);
    let days = window.days();
    let window_label = window.label();
    let stats = windowed_player_stats(db, key, lifetime, count_cache, window).await?;
    if page == 0 {
        let online = db.is_online(key, display).await?.is_some();
        return Ok(build_player_overview_embed(
            &stats,
            window_label,
            label,
            online,
            config,
            page,
            PLAYER_PAGES,
            elapsed,
        ));
    }

    let (kind, title, color, empty) = match page {
        1 => (
            EventKind::Chat,
            format!("Last {LOG_LIMIT} Messages by {display}"),
            PALETTE.advancement,
            "No messages in this window.",
        ),
        2 => (
            EventKind::Death,
            format!("Last {LOG_LIMIT} Deaths by {display}"),
            PALETTE.death,
            "No deaths in this window.",
        ),
        3 => (
            EventKind::Join,
            format!("Last {LOG_LIMIT} Joins by {display}"),
            PALETTE.join,
            "No joins in this window.",
        ),
        _ => (
            EventKind::Leave,
            format!("Last {LOG_LIMIT} Leaves by {display}"),
            PALETTE.leave,
            "No leaves in this window.",
        ),
    };
    if let std::collections::hash_map::Entry::Vacant(entry) = event_cache.entry((range_idx, page)) {
        let rows = load_player_events(db, key, display, kind, days).await?;
        entry.insert(rows);
    }
    let rows = event_cache
        .get(&(range_idx, page))
        .cloned()
        .unwrap_or_default();
    let lines: Vec<String> = rows
        .iter()
        .take(LOG_LIMIT as usize)
        .map(|row| {
            let text = match kind {
                EventKind::Chat => row.detail.as_deref().unwrap_or(""),
                EventKind::Death => row.detail.as_deref().unwrap_or("died"),
                EventKind::Join => "joined",
                EventKind::Leave => "left",
                _ => row.detail.as_deref().unwrap_or(""),
            };
            player_log_line(&row.player_name, row.occurred_at, text)
        })
        .collect();
    Ok(build_player_log_embed(
        display,
        &title,
        color,
        &lines,
        empty,
        label,
        window_label,
        page,
        PLAYER_PAGES,
        elapsed,
        config,
    ))
}

async fn windowed_player_stats(
    db: &crate::database::Db,
    key: &str,
    lifetime: &PlayerStats,
    cache: &mut std::collections::HashMap<u8, PlayerStats>,
    window: RangeChoice,
) -> Result<PlayerStats, Error> {
    let idx = window.as_index() as u8;
    if let Some(stats) = cache.get(&idx) {
        return Ok(stats.clone());
    }
    let mut stats = if let Some(days) = window.days() {
        db.player_stats_window(key, &lifetime.name, Some(days), None)
            .await?
    } else {
        lifetime.clone()
    };
    stats.uuid = lifetime.uuid.clone();
    stats.first_seen = lifetime.first_seen;
    stats.last_seen = lifetime.last_seen;
    stats.kill_rank = lifetime.kill_rank;
    stats.chat_rank = lifetime.chat_rank.clone();
    if window == RangeChoice::All {
        stats.last_message_at = lifetime.last_message_at;
    }
    cache.insert(idx, stats.clone());
    Ok(stats)
}

async fn load_player_events(
    db: &crate::database::Db,
    key: &str,
    name: &str,
    kind: EventKind,
    days: Option<i32>,
) -> Result<Vec<EventRow>, Error> {
    Ok(match kind {
        EventKind::Chat => db
            .chat_history(
                key,
                &ChatQuery {
                    player: Some(name.to_string()),
                    search: None,
                    limit: LOG_LIMIT,
                    offset: 0,
                    since_days: days,
                },
            )
            .await?
            .into_iter()
            .map(|r| EventRow {
                occurred_at: r.received_at,
                player_name: r.sender_name.unwrap_or_else(|| name.to_string()),
                detail: Some(r.plain_text),
            })
            .collect(),
        EventKind::Death => db.player_deaths(key, name, LOG_LIMIT, days).await?,
        EventKind::Join => {
            db.player_join_leave(key, name, "join", LOG_LIMIT, days)
                .await?
        }
        EventKind::Leave => {
            db.player_join_leave(key, name, "leave", LOG_LIMIT, days)
                .await?
        }
        _ => Vec::new(),
    })
}
