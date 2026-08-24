use crate::commands::message_log::send_message_log;
use crate::commands::shared::{
    autocomplete_player, autocomplete_server, player_name_arg, resolve_server_checked, RangeChoice,
};
use crate::presentation::ui::{notice, send_embed};
use crate::{Context, Error};

#[poise::command(slash_command)]
/// List chat lines that contain a letter or word
pub async fn keyword(
    ctx: Context<'_>,
    #[description = "Letter, word, or phrase to find"] text: String,
    #[description = "Only this player's messages"]
    #[autocomplete = "autocomplete_player"]
    player: Option<String>,
    #[description = "How far back"] range: Option<RangeChoice>,
    #[description = "Which server"]
    #[autocomplete = "autocomplete_server"]
    server: Option<String>,
) -> Result<(), Error> {
    let search = text.trim().to_string();
    if search.is_empty() {
        // Was silently showing an empty "Messages" log, which reads as "no
        // matches" rather than "you didn't give me anything to search for".
        return send_embed(
            ctx,
            notice(
                "Nothing to search for",
                "Give `/keyword` some text, e.g. `/keyword text:diamond`.",
            ),
        )
        .await;
    }

    let window = range.unwrap_or_default();
    let Some(server) = resolve_server_checked(ctx, server.as_deref(), player.as_deref()).await?
    else {
        return Ok(());
    };
    let name = player_name_arg(player.as_deref());
    send_message_log(ctx, server.key, server.label, name, Some(search), window).await
}
