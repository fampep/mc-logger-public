use std::sync::atomic::Ordering;

use poise::serenity_prelude::{self as serenity, ChannelType};
use poise::CreateReply;

use crate::presentation::ui::{send_embed, warning};
use crate::{Context, Error};

#[poise::command(
    slash_command,
    default_member_permissions = "ADMINISTRATOR",
    required_permissions = "ADMINISTRATOR",
    guild_only,
    subcommands("set", "clear", "status")
)]
/// Restrict which channel bot commands can be used in
pub async fn commandchannel(_ctx: Context<'_>) -> Result<(), Error> {
    Ok(())
}

#[poise::command(slash_command, required_permissions = "ADMINISTRATOR", guild_only)]
/// Only allow commands to run in this channel
pub async fn set(
    ctx: Context<'_>,
    #[description = "Channel commands must be run in"]
    #[channel_types("Text", "News")]
    channel: Option<serenity::model::channel::GuildChannel>,
) -> Result<(), Error> {
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
        .set_command_channel(Some(&channel.id.to_string()))
        .await?;
    ctx.data()
        .command_channel
        .store(channel.id.get(), Ordering::Relaxed);
    ctx.send(
        CreateReply::default()
            .content(format!(
                "Commands are now restricted to <#{}>. This command still works everywhere so you can change it later.",
                channel.id
            ))
            .ephemeral(true),
    )
    .await?;
    Ok(())
}

#[poise::command(slash_command, required_permissions = "ADMINISTRATOR", guild_only)]
/// Remove the restriction — commands work in any channel again
pub async fn clear(ctx: Context<'_>) -> Result<(), Error> {
    ctx.data().db.set_command_channel(None).await?;
    ctx.data().command_channel.store(0, Ordering::Relaxed);
    ctx.send(
        CreateReply::default()
            .content("Restriction removed — commands work in any channel again.")
            .ephemeral(true),
    )
    .await?;
    Ok(())
}

#[poise::command(slash_command, required_permissions = "ADMINISTRATOR", guild_only)]
/// Show the current restriction, if any
pub async fn status(ctx: Context<'_>) -> Result<(), Error> {
    let current = ctx.data().command_channel.load(Ordering::Relaxed);
    let msg = if current == 0 {
        "No restriction — commands work in any channel.".to_string()
    } else {
        format!("Commands are restricted to <#{current}>.")
    };
    ctx.send(CreateReply::default().content(msg).ephemeral(true))
        .await?;
    Ok(())
}
