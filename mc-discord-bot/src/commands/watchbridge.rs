use poise::serenity_prelude::{self as serenity, ChannelType};
use poise::CreateReply;

use crate::commands::shared::{autocomplete_player, autocomplete_server, player_name_arg, resolve_server_checked};
use crate::presentation::ui::{notice, send_embed, warning};
use crate::{Context, Error};

#[poise::command(
    slash_command,
    default_member_permissions = "ADMINISTRATOR",
    required_permissions = "ADMINISTRATOR",
    guild_only,
    subcommands("set", "add", "remove", "list")
)]
/// Post an embed here when a watched player joins or leaves
pub async fn watchbridge(_ctx: Context<'_>) -> Result<(), Error> {
    Ok(())
}

#[poise::command(slash_command, required_permissions = "ADMINISTRATOR", guild_only)]
/// Channel for the watchlist embeds
pub async fn set(
    ctx: Context<'_>,
    #[description = "Channel for watch embeds"]
    #[channel_types("Text", "News")]
    channel: Option<serenity::model::channel::GuildChannel>,
    #[description = "Which server"]
    #[autocomplete = "autocomplete_server"]
    server: Option<String>,
) -> Result<(), Error> {
    let Some(server) = resolve_server_checked(ctx, server.as_deref(), None).await? else {
        return Ok(());
    };
    let channel = match channel {
        Some(c) => c,
        None => match ctx.guild_channel().await {
            Some(c) if matches!(c.kind, ChannelType::Text | ChannelType::News) => c,
            _ => {
                return send_embed(
                    ctx,
                    warning("Need a channel", "Pick a text or announcement channel."),
                )
                .await;
            }
        },
    };
    ctx.data()
        .db
        .set_watchbridge_channel(&server.key, Some(&channel.id.to_string()))
        .await?;
    ctx.send(
        CreateReply::default()
            .content(format!(
                "Watchlist embeds for **{}** go to <#{}>. Add players with `/watchbridge add`.",
                server.label, channel.id
            ))
            .ephemeral(true),
    )
    .await?;
    Ok(())
}

#[poise::command(slash_command, required_permissions = "ADMINISTRATOR", guild_only)]
/// Add a player to the watchlist
pub async fn add(
    ctx: Context<'_>,
    #[description = "Player to watch"]
    #[autocomplete = "autocomplete_player"]
    player: String,
    #[description = "Which server"]
    #[autocomplete = "autocomplete_server"]
    server: Option<String>,
) -> Result<(), Error> {
    let Some(server) = resolve_server_checked(ctx, server.as_deref(), Some(player.as_str())).await?
    else {
        return Ok(());
    };
    let Some(name) = player_name_arg(Some(player.as_str())) else {
        return send_embed(ctx, warning("Need a player", "Pick a player to watch.")).await;
    };
    ctx.data()
        .db
        .add_watchbridge_player(&server.key, &name, &ctx.author().id.to_string())
        .await?;
    ctx.send(
        CreateReply::default()
            .content(format!("Watching **{name}** on **{}**.", server.label))
            .ephemeral(true),
    )
    .await?;
    Ok(())
}

#[poise::command(slash_command, required_permissions = "ADMINISTRATOR", guild_only)]
/// Remove a player from the watchlist
pub async fn remove(
    ctx: Context<'_>,
    #[description = "Player to stop watching"]
    #[autocomplete = "autocomplete_player"]
    player: String,
    #[description = "Which server"]
    #[autocomplete = "autocomplete_server"]
    server: Option<String>,
) -> Result<(), Error> {
    let Some(server) = resolve_server_checked(ctx, server.as_deref(), Some(player.as_str())).await?
    else {
        return Ok(());
    };
    let Some(name) = player_name_arg(Some(player.as_str())) else {
        return send_embed(ctx, warning("Need a player", "Pick a player to stop watching.")).await;
    };
    let removed = ctx
        .data()
        .db
        .remove_watchbridge_player(&server.key, &name)
        .await?;
    let content = if removed {
        format!("No longer watching **{name}** on **{}**.", server.label)
    } else {
        format!("**{name}** wasn't on the watchlist for **{}**.", server.label)
    };
    ctx.send(CreateReply::default().content(content).ephemeral(true))
        .await?;
    Ok(())
}

#[poise::command(slash_command, required_permissions = "ADMINISTRATOR", guild_only)]
/// Show the watchlist channel and players
pub async fn list(
    ctx: Context<'_>,
    #[description = "Which server"]
    #[autocomplete = "autocomplete_server"]
    server: Option<String>,
) -> Result<(), Error> {
    let Some(server) = resolve_server_checked(ctx, server.as_deref(), None).await? else {
        return Ok(());
    };
    let (channel, players) = tokio::try_join!(
        ctx.data().db.get_watchbridge_channel(&server.key),
        ctx.data().db.list_watchbridge_players(&server.key),
    )?;
    let channel_line = channel
        .map(|id| format!("<#{id}>"))
        .unwrap_or_else(|| "not set — run `/watchbridge set`".into());
    let players_line = if players.is_empty() {
        "none — add one with `/watchbridge add`".into()
    } else {
        players
            .iter()
            .map(|p| format!("**{p}**"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    send_embed(
        ctx,
        notice(
            &format!("Watchlist for {}", server.label),
            &format!("`Channel:` {channel_line}\n`Players:` {players_line}"),
        ),
    )
    .await
}
