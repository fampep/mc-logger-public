use crate::commands::message_log::send_message_log;
use crate::commands::shared::{
    autocomplete_player, autocomplete_server, player_name_arg, resolve_server_checked, RangeChoice,
};
use crate::{Context, Error};

#[poise::command(slash_command)]
/// Server chat log, or one player's messages
pub async fn chat(
    ctx: Context<'_>,
    #[description = "Only this player's messages — omit for everyone"]
    #[autocomplete = "autocomplete_player"]
    player: Option<String>,
    #[description = "How far back"] range: Option<RangeChoice>,
    #[description = "Which server"]
    #[autocomplete = "autocomplete_server"]
    server: Option<String>,
) -> Result<(), Error> {
    let window = range.unwrap_or_default();
    let Some(server) = resolve_server_checked(ctx, server.as_deref(), player.as_deref()).await?
    else {
        return Ok(());
    };
    let name = player_name_arg(player.as_deref());
    send_message_log(ctx, server.key, server.label, name, None, window).await
}
