use std::time::Instant;

use crate::commands::shared::{page_count_for, RangeChoice, EVENTS_PER_SCREEN};
use crate::database::ChatQuery;
use crate::presentation::embeds::{build_message_log_embed, player_log_line_marked};
use crate::presentation::ui::{clamp, send_nav_pager, NavPage, TIMEFRAME_SELECT};
use crate::{Context, Error};

pub async fn send_message_log(
    ctx: Context<'_>,
    server_key: String,
    server_label: String,
    player: Option<String>,
    search: Option<String>,
    window: RangeChoice,
) -> Result<(), Error> {
    let db = ctx.data().db.clone();
    let config = ctx.data().config.clone();
    let started = Instant::now();

    send_nav_pager(
        ctx,
        0,
        Some(&TIMEFRAME_SELECT),
        window.as_index(),
        move |page, range_idx| {
            let db = db.clone();
            let config = config.clone();
            let label = server_label.clone();
            let key = server_key.clone();
            let name = player.clone();
            let search = search.clone();
            async move {
                let window = RangeChoice::from_index(range_idx);
                let days = window.days();
                let window_label = window.label();
                let total = db
                    .count_chat_log(&key, name.as_deref(), search.as_deref(), days)
                    .await?;
                let pages = if total == 0 {
                    1
                } else {
                    page_count_for(total, EVENTS_PER_SCREEN)
                };
                let page = page.min(pages.saturating_sub(1));
                let rows = if total == 0 {
                    Vec::new()
                } else {
                    db.chat_log(
                        &key,
                        &ChatQuery {
                            player: name.clone(),
                            search: search.clone(),
                            limit: EVENTS_PER_SCREEN as i64,
                            offset: (page * EVENTS_PER_SCREEN) as i64,
                            since_days: days,
                        },
                    )
                    .await?
                };
                let lines: Vec<String> = rows
                    .iter()
                    .map(|r| {
                        player_log_line_marked(
                            r.sender_name.as_deref().unwrap_or("?"),
                            r.received_at,
                            &r.plain_text,
                            search.as_deref(),
                        )
                    })
                    .collect();
                Ok(NavPage {
                    embed: build_message_log_embed(
                        &log_title(name.as_deref(), search.as_deref()),
                        &lines,
                        &label,
                        window_label,
                        page,
                        pages,
                        started.elapsed(),
                        name.as_deref(),
                        Some(&config),
                    ),
                    page_count: pages,
                })
            }
        },
    )
    .await
}

fn log_title(player: Option<&str>, search: Option<&str>) -> String {
    let query = search.map(|q| format!("`{}`", clamp(&q.replace('`', ""), 40)));
    match (player, query.as_deref()) {
        (Some(name), Some(query)) => format!("{query} in {name}'s messages"),
        (None, Some(query)) => format!("Messages matching {query}"),
        (Some(name), None) => format!("{name}'s messages"),
        (None, None) => "Messages".into(),
    }
}
