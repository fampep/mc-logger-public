use crate::presentation::embeds::build_help_embed;
use crate::presentation::ui::send_embed;
use crate::{Context, Error};

#[poise::command(slash_command)]
/// List of commands
pub async fn help(ctx: Context<'_>) -> Result<(), Error> {
    let mut groups = vec![
        (
            "Look things up".into(),
            vec![
                (
                    "/stats".into(),
                    "player or server numbers; 30d / 60d / 90d / all".into(),
                ),
                (
                    "/keyword".into(),
                    "chat lines that contain a letter or word".into(),
                ),
                (
                    "/chat".into(),
                    "server chat log, or one player's messages".into(),
                ),
                (
                    "/leaderboard".into(),
                    "top kills, deaths, chat, joins, playtime".into(),
                ),
            ],
        ),
        (
            "Feed".into(),
            vec![
                (
                    "/chatbridge".into(),
                    "live chat feed (needs Administrator)".into(),
                ),
                (
                    "/chatbridge set".into(),
                    "point the feed at a channel — start here".into(),
                ),
                (
                    "/chatbridge customize".into(),
                    "feed style, rainbow mode, which events post and where".into(),
                ),
                (
                    "/chatbridge reset".into(),
                    "restore default kinds and routes; keep the main channel".into(),
                ),
                (
                    "/chatbridge remove".into(),
                    "disable the feed and clear it until set up again".into(),
                ),
                (
                    "/watchbridge".into(),
                    "embed here when a watched player joins/leaves (needs Administrator)".into(),
                ),
                ("/watchbridge set".into(), "point it at a channel".into()),
                (
                    "/watchbridge add".into(),
                    "add a player to the watchlist".into(),
                ),
            ],
        ),
        (
            "Server".into(),
            vec![
                ("/database".into(), "stored totals and disk size".into()),
                (
                    "/commandchannel".into(),
                    "restrict commands to one channel (needs Administrator)".into(),
                ),
            ],
        ),
    ];
    if ctx.data().config.multi_server() {
        groups.push((
            "Multiple servers".into(),
            vec![
                ("server:".into(), "pick which log on any command".into()),
                (
                    "player search".into(),
                    "shows Name · Server in autocomplete".into(),
                ),
            ],
        ));
    }
    send_embed(ctx, build_help_embed(&groups)).await
}
