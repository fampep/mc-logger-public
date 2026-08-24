use std::time::Instant;

use crate::commands::shared::{autocomplete_server, resolve_server_checked};
use crate::presentation::embeds::build_database_embed;
use crate::presentation::ui::{send_nav_pager, NavPage};
use crate::{Context, Error};

#[poise::command(slash_command)]
/// Database size and stored totals
pub async fn database(
    ctx: Context<'_>,
    #[description = "Which server"]
    #[autocomplete = "autocomplete_server"]
    server: Option<String>,
) -> Result<(), Error> {
    let started = Instant::now();
    let Some(server) = resolve_server_checked(ctx, server.as_deref(), None).await? else {
        return Ok(());
    };
    let stats = ctx.data().db.database_stats(&server.key).await?;
    send_nav_pager(ctx, 0, None, 0, move |_, _| {
        let embed = build_database_embed(&stats, &server.label, started.elapsed());
        async move { Ok(NavPage::one(embed)) }
    })
    .await
}
