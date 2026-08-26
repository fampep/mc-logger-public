use poise::serenity_prelude::{CreateEmbed, CreateEmbedAuthor, CreateEmbedFooter};

use crate::config::Config;
use crate::database::{
    ChatRow, DatabaseStats, EventKind, EventRow, LeaderMetric, LeaderRow, PlayerStats, WindowStats,
    MIN_KILLS_FOR_KD,
};
use crate::presentation::ui::{
    bold, clamp, code, compact, date_only, date_time, duration_hm, escape_md, fmt, footer, italic,
    join_lines, kind_color, log_text, overview_line, overview_num, player_name, ratio, short_time,
    skeleton, table, with_context, Category, Column, Limits, PALETTE,
};

fn subject_of(row: &ChatRow) -> Option<&str> {
    row.sender_name.as_deref().or(row.subject_name.as_deref())
}

fn author_of(row: &ChatRow) -> Option<&str> {
    if matches!(row.kind.as_str(), "chat" | "c" | "whisper" | "w") {
        row.sender_label
            .as_deref()
            .or(row.sender_name.as_deref())
            .or(row.subject_name.as_deref())
    } else {
        row.sender_name.as_deref().or(row.subject_name.as_deref())
    }
}

fn body_of(row: &ChatRow) -> String {
    let name = row
        .subject_name
        .as_deref()
        .or(row.sender_name.as_deref())
        .unwrap_or("?");
    let text = row.plain_text.trim();
    match row.kind.as_str() {
        "chat" | "c" | "whisper" | "w" | "s" | "server" | "u" | "unknown" => {
            if text.is_empty() {
                " ".into()
            } else {
                row.plain_text.clone()
            }
        }
        "j" | "join" => {
            if !text.is_empty() {
                row.plain_text.clone()
            } else {
                format!("{name} joined the game")
            }
        }
        "l" | "leave" => {
            if !text.is_empty() {
                row.plain_text.clone()
            } else {
                format!("{name} left the game")
            }
        }
        "d" | "death" => reconstruct_death(name, row.killer_name.as_deref(), text),
        "a" | "advancement" => reconstruct_advancement(name, text),
        _ => {
            if text.is_empty() {
                " ".into()
            } else {
                row.plain_text.clone()
            }
        }
    }
}

fn reconstruct_death(name: &str, killer: Option<&str>, stored: &str) -> String {
    // Historical rows kept the full vanilla sentence; compact rows store ''.
    if !stored.is_empty() && stored.contains(name) {
        return stored.to_string();
    }
    match killer.filter(|k| !k.is_empty()) {
        Some(killer) => format!("{name} was slain by {killer}"),
        None => format!("{name} died"),
    }
}

fn reconstruct_advancement(name: &str, stored: &str) -> String {
    if stored.is_empty() {
        return format!("{name} has made an advancement");
    }
    if stored.contains(name)
        && (stored.contains("advancement")
            || stored.contains("challenge")
            || stored.contains("goal"))
    {
        return stored.to_string();
    }
    let title = stored.trim();
    format!("{name} has made the advancement {title}")
}

/// in bold at the head of the description, the player's head as the small
/// footer icon beside the timestamp, and one accent colour per kind down the
/// left edge. Chat uses the background colour so the stripe disappears and only
/// events — deaths, joins, leaves — draw the eye.
/// Author line with the player's head, addressed by name.
fn author_with_head(name: &str, config: &Config) -> CreateEmbedAuthor {
    CreateEmbedAuthor::new(name).icon_url(config.head_url_name(name))
}

pub fn build_event_embed(row: &ChatRow, head_url: Option<&str>, rainbow: bool) -> CreateEmbed {
    let kind = row.kind.as_str();
    let body = escape_md(&body_of(row));
    let speaker = escape_md(author_of(row).unwrap_or(kind));

    let description = match kind {
        "chat" | "c" => format!("{} {body}", bold(&format!("{speaker}:"))),
        "whisper" | "w" => format!("{} {}", bold(&format!("{speaker}:")), italic(&body)),
        // Deaths, joins, leaves and advancements already read as a sentence
        // starting with the player, so only the name needs weight.
        _ => lead_with_name(subject_of(row), &body),
    };

    // Rainbow mode deliberately overrides the meaningful per-kind colour
    // (deaths red, joins green, etc.) — that's the point of turning it on.
    let colour = if rainbow {
        crate::presentation::ui::rainbow_color(row.received_at)
    } else {
        feed_colour(kind)
    };
    let mut embed = CreateEmbed::new()
        .colour(colour)
        .timestamp(row.received_at)
        .description(clamp(&description, Limits::DESCRIPTION));

    // A zero-width space, not "": Discord drops a footer whose text is empty,
    // and the icon goes with it — which also left the attached fallback head
    // unreferenced, so it rendered as a loose image below the embed.
    let mut footer = CreateEmbedFooter::new("\u{200b}");
    if let Some(head) = head_url {
        footer = footer.icon_url(head);
    }
    embed = embed.footer(footer);
    embed
}

/// Death and presence lines already begin with the player's name; bold that
/// prefix instead of repeating it.
fn lead_with_name(subject: Option<&str>, body: &str) -> String {
    let Some(name) = subject else {
        return body.to_string();
    };
    let escaped = escape_md(name);
    match body.strip_prefix(escaped.as_str()) {
        Some(rest) => format!("{}{rest}", bold(&escaped)),
        None => body.to_string(),
    }
}

/// Chat sits on the embed background so its stripe vanishes; everything else
/// keeps the palette's accent.
fn feed_colour(kind: &str) -> u32 {
    match kind {
        "chat" | "c" => PALETTE.muted,
        _ => kind_color(kind),
    }
}

pub fn feed_line(row: &ChatRow) -> String {
    let when = short_time(row.received_at);
    let body = log_text(&body_of(row));
    let name = author_of(row);
    let head = subject_of(row);
    match row.kind.as_str() {
        "chat" | "c" => {
            if name.is_some() && name != head {
                format!("{when} {}  {body}", player_name(name))
            } else {
                format!("{when} {}  {body}", player_name(head))
            }
        }
        "whisper" | "w" => format!(
            "{when} {}  {}",
            player_name(name),
            italic(&format!("whispers: {body}"))
        ),
        _ => format!("{when} {}", italic(&body)),
    }
}

pub fn pack_feed(rows: &[ChatRow], max_lines: usize) -> Vec<Vec<ChatRow>> {
    let mut groups = Vec::new();
    let mut batch = Vec::new();
    let mut budget = 0usize;
    for row in rows {
        let cost = feed_line(row).len() + 1;
        if !batch.is_empty() && (batch.len() >= max_lines || budget + cost > Limits::DESCRIPTION) {
            groups.push(std::mem::take(&mut batch));
            budget = 0;
        }
        batch.push(row.clone());
        budget += cost;
    }
    if !batch.is_empty() {
        groups.push(batch);
    }
    groups
}

pub fn build_feed_batch_embed(rows: &[ChatRow], server_label: &str, rainbow: bool) -> CreateEmbed {
    let host = rows
        .iter()
        .find_map(|r| r.server_host.as_deref())
        .unwrap_or(server_label);
    let lines: Vec<_> = rows.iter().map(feed_line).collect();
    let ts = rows
        .last()
        .map(|r| r.received_at)
        .unwrap_or_else(chrono::Utc::now);
    let colour = if rainbow {
        crate::presentation::ui::rainbow_color(ts)
    } else {
        PALETTE.brand
    };
    CreateEmbed::new()
        .colour(colour)
        .description(join_lines(&lines, Limits::DESCRIPTION))
        .footer(CreateEmbedFooter::new(footer(
            host,
            &[&format!(
                "{} {}",
                rows.len(),
                if rows.len() == 1 { "line" } else { "lines" }
            )],
        )))
        .timestamp(ts)
}

fn rank_table(rows: &[LeaderRow], value_header: &str) -> String {
    table(
        &[
            Column::new("#").right(),
            Column::new("player").width(16),
            Column::new(value_header).right(),
        ],
        &rows
            .iter()
            .enumerate()
            .map(|(i, row)| vec![format!("{}", i + 1), row.name.clone(), fmt(row.value)])
            .collect::<Vec<_>>(),
        Limits::FIELD,
    )
}

pub fn build_top_boards_embed(
    server_label: &str,
    top_chatters: &[LeaderRow],
    top_killers: &[LeaderRow],
    top_deaths: &[LeaderRow],
    top_kd: &[LeaderRow],
) -> CreateEmbed {
    let embed = skeleton(Category::Stats)
        .title(format!("{server_label} — top players"))
        .description("Preview of `/leaderboard`. Open that command for the full boards.");
    let boards = [
        ("Most talkative", top_chatters, "lines"),
        ("Deadliest", top_killers, "count"),
        ("Most deaths", top_deaths, "count"),
        ("Highest K/D", top_kd, "K/D"),
    ];
    let mut embed = embed;
    for (title, rows, header) in boards {
        let value = if rows.is_empty() {
            italic("Nothing here yet.")
        } else {
            rank_table(rows, header)
        };
        embed = embed.field(title, value, true);
    }
    with_context(embed, server_label, &["full boards in /leaderboard"])
}

fn event_title(kind: EventKind) -> &'static str {
    match kind {
        EventKind::Chat => "Messages",
        EventKind::Join => "Joins",
        EventKind::Leave => "Leaves",
        EventKind::Death => "Deaths",
        EventKind::Kill => "Kills",
        EventKind::Advancement => "Advancements",
    }
}

fn event_page_color(kind: EventKind) -> u32 {
    match kind {
        EventKind::Chat => PALETTE.advancement,
        EventKind::Join => PALETTE.join,
        EventKind::Leave => PALETTE.leave,
        EventKind::Death | EventKind::Kill => PALETTE.death,
        EventKind::Advancement => PALETTE.advancement,
    }
}

fn event_empty(kind: EventKind) -> &'static str {
    match kind {
        EventKind::Chat => "No messages in this window.",
        EventKind::Join => "No joins in this window.",
        EventKind::Leave => "No leaves in this window.",
        EventKind::Death => "No deaths in this window.",
        EventKind::Kill => "No kills in this window.",
        EventKind::Advancement => "No advancements in this window.",
    }
}

fn event_line_text(kind: EventKind, row: &EventRow) -> String {
    match kind {
        EventKind::Chat => row.detail.clone().unwrap_or_default(),
        EventKind::Death => row.detail.clone().unwrap_or_else(|| "died".into()),
        EventKind::Join => "joined".into(),
        EventKind::Leave => "left".into(),
        EventKind::Kill => format!("killed {}", row.detail.as_deref().unwrap_or("?")),
        EventKind::Advancement => row
            .detail
            .clone()
            .unwrap_or_else(|| row.player_name.clone()),
    }
}

pub fn event_log_lines(kind: EventKind, rows: &[EventRow]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            player_log_line(
                &row.player_name,
                row.occurred_at,
                &event_line_text(kind, row),
            )
        })
        .collect()
}

fn trend_suffix(current: i64, previous: Option<i64>, window_label: &str) -> String {
    match previous {
        Some(prev) if window_label != "all time" => {
            format!(" · {}", crate::presentation::ui::trend_i64(current, prev))
        }
        _ => String::new(),
    }
}

pub fn build_stats_overview_embed(
    server_label: &str,
    window_label: &str,
    current: &WindowStats,
    previous: Option<&WindowStats>,
    online: i64,
    last_seen: Option<chrono::DateTime<chrono::Utc>>,
) -> CreateEmbed {
    let trend = |current: i64, prev_field: fn(&WindowStats) -> i64| {
        format!(
            "**{}**{}",
            fmt(current as f64),
            trend_suffix(current, previous.map(prev_field), window_label)
        )
    };
    // Grouped state / activity / combat / meta, same order and wording as the
    // per-player overview below — one visual grammar for both, not two.
    let mut lines = vec![
        overview_num("Online", online),
        overview_num("Players", current.players),
        String::new(),
        overview_line("Messages", trend(current.chat, |p| p.chat)),
        overview_line("Joins", trend(current.joins, |p| p.joins)),
        overview_line("Leaves", trend(current.leaves, |p| p.leaves)),
        overview_num("Advancements", current.goals),
        String::new(),
        overview_line("Deaths", trend(current.deaths, |p| p.deaths)),
        overview_line("PvP kills", trend(current.kills, |p| p.kills)),
    ];
    if let Some(t) = last_seen {
        lines.push(String::new());
        lines.push(overview_line("Latest", format!("**{}**", date_time(t))));
    }
    CreateEmbed::new()
        .colour(PALETTE.brand)
        .title(format!("{server_label}'s stats"))
        .description(lines.join("\n"))
        .footer(CreateEmbedFooter::new(footer(
            server_label,
            &[window_label, "tabs for recent events"],
        )))
        .timestamp(chrono::Utc::now())
}

#[allow(clippy::too_many_arguments)] // Embed builders mirror independent Discord fields.
pub fn build_events_page_embed(
    kind: EventKind,
    rows: &[EventRow],
    server_label: &str,
    window_label: &str,
    page: usize,
    page_count: usize,
    elapsed: std::time::Duration,
    player: Option<&str>,
    config: Option<&Config>,
) -> CreateEmbed {
    let lines = event_log_lines(kind, rows);
    let body = if lines.is_empty() {
        italic(event_empty(kind))
    } else {
        join_lines(&lines, Limits::DESCRIPTION)
    };
    let title = match player {
        Some(name) => format!("{name}'s {}", event_title(kind).to_lowercase()),
        None => event_title(kind).to_string(),
    };
    let mut embed = CreateEmbed::new()
        .colour(event_page_color(kind))
        .title(title)
        .description(body)
        .footer(CreateEmbedFooter::new(pager_footer(
            page,
            page_count,
            server_label,
            window_label,
            elapsed,
        )))
        .timestamp(chrono::Utc::now());
    if let (Some(name), Some(config)) = (player, config) {
        embed = embed.author(author_with_head(name, config));
    }
    embed
}

#[allow(clippy::too_many_arguments)] // Embed builders mirror independent Discord fields.
pub fn build_player_overview_embed(
    stats: &PlayerStats,
    window_label: &str,
    server_label: &str,
    online: bool,
    config: &Config,
    page: usize,
    page_count: usize,
    elapsed: std::time::Duration,
) -> CreateEmbed {
    let name = &stats.name;
    let rank = stats
        .chat_rank
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| format!("**{s}**"))
        .unwrap_or_else(|| "**None**".into());
    let joined = stats
        .first_seen
        .map(|t| format!("**{}**", date_only(t)))
        .unwrap_or_else(|| "**Unknown**".into());
    let online_line = if online { "✅ **Yes**" } else { "❌ **No**" };
    // Discord renders <t:unix:R> as "3 hours ago" and keeps it ticking, which
    // is what you want from "last seen". Deliberately unbounded by the chosen
    // window: asking for 7d should still say they were last here in March.
    let last_seen = if online {
        "**Now**".to_string()
    } else {
        stats
            .last_seen
            .map(|t| format!("**<t:{}:R>** (<t:{}:f>)", t.timestamp(), t.timestamp()))
            .unwrap_or_else(|| "**Never seen**".into())
    };
    // Code-formatted rather than bold like the other meta fields — a UUID is
    // something people copy out, not read.
    let uuid_line = stats
        .uuid
        .as_deref()
        .map(code)
        .unwrap_or_else(|| "**Unknown**".into());
    // Same grouping and wording as the server-wide overview — the name is
    // already on the author line and title, so labels don't repeat it.
    let body = format!(
        "`Online:` {online_line}\n\
`Playtime:` **{}**\n\
\n\
`Messages:` **{}**\n\
`Joins:` **{}**\n\
`Leaves:` **{}**\n\
\n\
`Deaths:` **{}**\n\
`PvP kills:` **{}**\n\
`Advancements:` **{}**\n\
\n\
`Chat rank:` {rank}\n\
`Join date:` {joined}\n\
`Last seen:` {last_seen}\n\
`UUID:` {uuid_line}",
        duration_hm(stats.playtime_secs),
        fmt(stats.messages as f64),
        fmt(stats.joins as f64),
        fmt(stats.leaves as f64),
        fmt(stats.deaths as f64),
        fmt(stats.kills as f64),
        fmt(stats.advancements as f64),
    );
    CreateEmbed::new()
        .colour(PALETTE.brand)
        .author(author_with_head(name, config))
        .title(format!("{name}'s stats"))
        .thumbnail(config.body_url_name(name))
        .description(body)
        .footer(CreateEmbedFooter::new(pager_footer(
            page,
            page_count,
            server_label,
            window_label,
            elapsed,
        )))
        .timestamp(chrono::Utc::now())
}

#[allow(clippy::too_many_arguments)] // Embed builders mirror independent Discord fields.
pub fn build_player_log_embed(
    name: &str,
    title: &str,
    color: u32,
    lines: &[String],
    empty: &str,
    server_label: &str,
    window_label: &str,
    page: usize,
    page_count: usize,
    elapsed: std::time::Duration,
    config: &Config,
) -> CreateEmbed {
    let body = if lines.is_empty() {
        italic(empty)
    } else {
        join_lines(lines, Limits::DESCRIPTION)
    };
    CreateEmbed::new()
        .colour(color)
        .author(author_with_head(name, config))
        .title(title)
        .description(body)
        .footer(CreateEmbedFooter::new(pager_footer(
            page,
            page_count,
            server_label,
            window_label,
            elapsed,
        )))
        .timestamp(chrono::Utc::now())
}

#[allow(clippy::too_many_arguments)] // Embed builders mirror independent Discord fields.
pub fn build_message_log_embed(
    title: &str,
    lines: &[String],
    server_label: &str,
    window_label: &str,
    page: usize,
    page_count: usize,
    elapsed: std::time::Duration,
    player: Option<&str>,
    config: Option<&Config>,
) -> CreateEmbed {
    let body = if lines.is_empty() {
        italic("No messages in this window.")
    } else {
        join_lines(lines, Limits::DESCRIPTION)
    };
    let mut embed = CreateEmbed::new()
        .colour(PALETTE.advancement)
        .title(title)
        .description(body)
        .footer(CreateEmbedFooter::new(pager_footer(
            page,
            page_count,
            server_label,
            window_label,
            elapsed,
        )))
        .timestamp(chrono::Utc::now());
    if let (Some(name), Some(config)) = (player, config) {
        embed = embed
            .author(author_with_head(name, config))
            .thumbnail(config.body_url_name(name));
    }
    embed
}

pub fn player_log_line(name: &str, at: chrono::DateTime<chrono::Utc>, text: &str) -> String {
    player_log_line_marked(name, at, text, None)
}

pub fn player_log_line_marked(
    name: &str,
    at: chrono::DateTime<chrono::Utc>,
    text: &str,
    needle: Option<&str>,
) -> String {
    format!(
        "{} {} » {}",
        date_time(at),
        code(name),
        highlight_clip(&clamp(text, 80), needle)
    )
}

fn highlight_clip(text: &str, needle: Option<&str>) -> String {
    let Some(needle) = needle.map(str::trim).filter(|s| !s.is_empty()) else {
        return text.to_string();
    };
    let messy = |s: &str| {
        s.chars()
            .any(|c| matches!(c, '*' | '_' | '`' | '|' | '~' | '\\'))
    };
    if messy(needle) || messy(text) {
        return text.to_string();
    }
    let hay = text.to_ascii_lowercase();
    let pin = needle.to_ascii_lowercase();
    let Some(start) = hay.find(&pin) else {
        return text.to_string();
    };
    let end = start + pin.len();
    if !text.is_char_boundary(start) || !text.is_char_boundary(end) {
        return text.to_string();
    }
    format!(
        "{}**{}**{}",
        &text[..start],
        &text[start..end],
        &text[end..]
    )
}

pub fn pager_footer(
    page: usize,
    page_count: usize,
    server_label: &str,
    window_label: &str,
    elapsed: std::time::Duration,
) -> String {
    footer(
        server_label,
        &[
            window_label,
            &format!("Page {}/{}", page + 1, page_count.max(1)),
            &format!("Done in {:.2}s", elapsed.as_secs_f64()),
        ],
    )
}

#[allow(clippy::too_many_arguments)] // Embed builders mirror independent Discord fields.
pub fn build_leaderboard_embed(
    metric: LeaderMetric,
    rows: &[LeaderRow],
    server_label: &str,
    page: usize,
    page_count: usize,
    total: i64,
    per_page: usize,
    elapsed: std::time::Duration,
    config: &Config,
) -> CreateEmbed {
    let start_rank = page * per_page + 1;
    let title = match metric {
        LeaderMetric::Kills => "PvP kills",
        LeaderMetric::Deaths => "Deaths",
        LeaderMetric::Kd => "Kill/death ratio",
        LeaderMetric::Messages => "Messages sent",
        LeaderMetric::Joins => "Server joins",
        LeaderMetric::Playtime => "Total playtime",
    };
    let note = match metric {
        LeaderMetric::Kills => "player-versus-player only",
        LeaderMetric::Deaths => "mobs and environment included",
        LeaderMetric::Kd => "",
        LeaderMetric::Messages => "public chat only",
        LeaderMetric::Joins => "from the in-game tab list",
        LeaderMetric::Playtime => "time logged in, lifetime",
    };
    let kd_note = format!("at least {MIN_KILLS_FOR_KD} kills to qualify");
    let note = if matches!(metric, LeaderMetric::Kd) {
        kd_note.as_str()
    } else {
        note
    };

    let lines: Vec<String> = if rows.is_empty() {
        vec![italic("Nothing on this board yet.")]
    } else {
        rows.iter()
            .enumerate()
            .map(|(i, row)| {
                let rank = start_rank + i;
                if matches!(metric, LeaderMetric::Kd) {
                    let kills = row.kills.unwrap_or(0);
                    let deaths = row.deaths.unwrap_or(0);
                    format!(
                        "`#{rank}` **{}** — **{}** · {} / {}",
                        row.name,
                        ratio(kills, deaths),
                        fmt(kills as f64),
                        fmt(deaths as f64)
                    )
                } else if matches!(metric, LeaderMetric::Kills) {
                    let kills = row.kills.unwrap_or(row.value as i64);
                    let deaths = row.deaths.unwrap_or(0);
                    format!(
                        "`#{rank}` **{}** — **{}** · {} deaths",
                        row.name,
                        fmt(kills as f64),
                        fmt(deaths as f64)
                    )
                } else if matches!(metric, LeaderMetric::Playtime) {
                    format!(
                        "`#{rank}` **{}** — **{}**",
                        row.name,
                        duration_hm(row.value)
                    )
                } else {
                    format!("`#{rank}` **{}** — **{}**", row.name, fmt(row.value))
                }
            })
            .collect()
    };

    let mut embed = CreateEmbed::new()
        .colour(PALETTE.brand)
        .title(title)
        .description(join_lines(&lines, Limits::DESCRIPTION))
        .footer(CreateEmbedFooter::new(pager_footer(
            page,
            page_count,
            server_label,
            &format!("{} ranked · {note}", compact(total)),
            elapsed,
        )))
        .timestamp(chrono::Utc::now());
    if let Some(first) = rows.first() {
        embed = embed
            .author(author_with_head(&first.name, config))
            .thumbnail(config.body_url_name(&first.name));
    }
    embed
}

pub fn build_database_embed(
    stats: &DatabaseStats,
    server_label: &str,
    elapsed: std::time::Duration,
) -> CreateEmbed {
    let mut lines = vec![
        overview_line("Database", format!("**{}**", stats.database)),
        overview_line("On disk", format!("**{}**", stats.size)),
        overview_num("Players", stats.players),
        overview_num("Messages", stats.chat),
        overview_num("Joins", stats.joins),
        overview_num("Leaves", stats.leaves),
        overview_num("Deaths", stats.deaths),
        overview_num("PvP kills", stats.kills),
        overview_num("Advancements", stats.goals),
        overview_num("Sessions", stats.sessions),
    ];
    if let Some(t) = stats.oldest {
        lines.push(overview_line("Since", format!("**{}**", date_time(t))));
    }
    if let Some(t) = stats.newest {
        lines.push(overview_line("Newest", format!("**{}**", date_time(t))));
    }
    CreateEmbed::new()
        .colour(PALETTE.brand)
        .title(format!("{server_label}'s stored data"))
        .description(lines.join("\n"))
        .footer(CreateEmbedFooter::new(pager_footer(
            0,
            1,
            server_label,
            "",
            elapsed,
        )))
        .timestamp(chrono::Utc::now())
}
pub type BridgeStatusRow = (String, Option<String>, bool, Option<String>, Option<i64>);

pub fn build_bridge_status_embed(rows: &[BridgeStatusRow]) -> CreateEmbed {
    let mut embed = skeleton(Category::Admin).title("Chat feed");
    for (label, channel_id, enabled, error, behind) in rows {
        let value = if let Some(err) = error {
            format!("Database unreachable — {}", code(clamp(err, 200)))
        } else if let Some(ch) = channel_id {
            let mut parts = vec![format!(
                "{} in <#{ch}>",
                bold(if *enabled { "Active" } else { "Paused" })
            )];
            if behind.unwrap_or(0) > 0 {
                parts.push(format!("{} lines behind", fmt(behind.unwrap_or(0) as f64)));
            } else {
                parts.push("up to date".into());
            }
            parts.join("\n")
        } else {
            format!("No channel set — run {}", code("/chatbridge set"))
        };
        embed = embed.field(label, value, false);
    }
    embed.footer(CreateEmbedFooter::new(
        "Paused feeds keep logging; they just stop posting",
    ))
}

pub fn build_help_embed(groups: &[(String, Vec<(String, String)>)]) -> CreateEmbed {
    let mut lines = vec![
        "Commands read the logger database. Live feeds post into a channel.".into(),
        "Player and log replies use `<<` `<` `>` `>>` `X` and a timeframe menu.".into(),
        String::new(),
    ];
    for (group, entries) in groups {
        lines.push(format!("**{group}**"));
        for (name, desc) in entries {
            lines.push(overview_line(name, format!("**{desc}**")));
        }
        lines.push(String::new());
    }
    CreateEmbed::new()
        .colour(PALETTE.brand)
        .title("Commands")
        .description(join_lines(&lines, Limits::DESCRIPTION))
        .footer(CreateEmbedFooter::new(
            "Names autocomplete across every logged server",
        ))
        .timestamp(chrono::Utc::now())
}

/// How the live feed is currently being fed, for the notice posted into the
/// bridge channel on startup and whenever the gateway link changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedLink {
    /// Subscribed to the terminal-client gateway.
    Streaming,
    /// Gateway configured but unreachable; the bot is retrying.
    GatewayDown,
    /// Gateway link came back.
    Recovered,
    /// No gateway configured — reading new rows from Postgres.
    Polling,
    /// The bot itself dropped off Discord and came back.
    BotReconnected,
}

pub fn build_feed_link_embed(server_label: &str, link: FeedLink, detail: &str) -> CreateEmbed {
    let (colour, title) = match link {
        FeedLink::Streaming => (PALETTE.join, "Live feed connected"),
        FeedLink::Recovered => (PALETTE.join, "Live feed reconnected"),
        FeedLink::GatewayDown => (PALETTE.leave, "Live feed disconnected"),
        FeedLink::Polling => (PALETTE.neutral, "Live feed polling the database"),
        FeedLink::BotReconnected => (PALETTE.join, "Bot reconnected"),
    };
    CreateEmbed::new()
        .colour(colour)
        .title(title)
        .description(clamp(detail, Limits::DESCRIPTION))
        .footer(CreateEmbedFooter::new(format!(
            "{server_label} · event gateway"
        )))
        .timestamp(chrono::Utc::now())
}
