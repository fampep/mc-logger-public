use std::time::Instant;

use crate::commands::shared::{
    autocomplete_server, page_count_for, resolve_server_checked, BoardChoice,
};
use crate::database::LeaderMetric;
use crate::presentation::embeds::build_leaderboard_embed;
use crate::presentation::ui::{send_nav_pager, NavPage, BOARD_SELECT};
use crate::{Context, Error};

#[poise::command(slash_command)]
/// Rank players by kills, K/D, deaths, messages, joins, or playtime
pub async fn leaderboard(
    ctx: Context<'_>,
    #[description = "Which board"] board: Option<BoardChoice>,
    #[description = "Rows per page"]
    #[min = 5]
    #[max = 25]
    per_page: Option<i64>,
    #[description = "Which server"]
    #[autocomplete = "autocomplete_server"]
    server: Option<String>,
) -> Result<(), Error> {
    let Some(server) = resolve_server_checked(ctx, server.as_deref(), None).await? else {
        return Ok(());
    };
    let start = board.unwrap_or_default().as_index();
    let per_page = per_page.unwrap_or(15) as usize;
    let metrics = [
        LeaderMetric::Kills,
        LeaderMetric::Kd,
        LeaderMetric::Deaths,
        LeaderMetric::Messages,
        LeaderMetric::Joins,
        LeaderMetric::Playtime,
    ];
    let db = ctx.data().db.clone();
    let config = ctx.data().config.clone();
    let label = server.label.clone();
    let key = server.key.clone();
    let started = Instant::now();

    send_nav_pager(
        ctx,
        0,
        Some(&BOARD_SELECT),
        start,
        move |page, select_idx| {
            let db = db.clone();
            let config = config.clone();
            let label = label.clone();
            let key = key.clone();
            async move {
                let metric = metrics[select_idx.min(metrics.len() - 1)];
                let total = db.count_leaderboard(&key, metric).await?;
                let pages = page_count_for(total, per_page);
                let page = page.min(pages.saturating_sub(1));
                let rows = db
                    .leaderboard(&key, metric, per_page as i64, (page * per_page) as i64)
                    .await?;
                Ok(NavPage {
                    embed: build_leaderboard_embed(
                        metric,
                        &rows,
                        &label,
                        page,
                        pages,
                        total,
                        per_page,
                        started.elapsed(),
                        &config,
                    ),
                    page_count: pages,
                })
            }
        },
    )
    .await
}
