use poise::serenity_prelude::{self as serenity, ChannelType};
use poise::CreateReply;

use crate::commands::shared::{autocomplete_server, resolve_server_checked};
use crate::discord::topic::TopicResult;
use crate::presentation::embeds::build_bridge_status_embed;
use crate::presentation::ui::{send_embed, warning};
use crate::{Context, Error};

#[poise::command(
    slash_command,
    default_member_permissions = "ADMINISTRATOR",
    required_permissions = "ADMINISTRATOR",
    // poise treats DMs as "all permissions granted" (there's no guild role to
    // check), so `required_permissions` alone does not stop a DM from
    // bypassing every admin gate below. `guild_only` closes that: the
    // permission check runs for this parent on every subcommand invocation,
    // so setting it once here is enough, but it's repeated on each
    // subcommand too — same reasoning as `required_permissions` already
    // being repeated on all of them.
    guild_only,
    subcommands(
        "set",
        "status",
        "pause",
        "resume",
        "topic",
        "test",
        "customize",
        "reset",
        "remove"
    )
)]
/// Set up the live chat feed
pub async fn chatbridge(_ctx: Context<'_>) -> Result<(), Error> {
    Ok(())
}

#[poise::command(slash_command, required_permissions = "ADMINISTRATOR", guild_only)]
pub async fn set(
    ctx: Context<'_>,
    #[description = "Channel for the feed"]
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
        .set_bridge_channel(&server.key, Some(&channel.id.to_string()))
        .await?;
    ctx.data().db.set_bridge_enabled(&server.key, true).await?;
    ctx.data()
        .db
        .ensure_event_routes(&server.key, &ctx.data().config.bridge.kinds)
        .await?;
    if let Some(bridge) = ctx.data().bridges.get(&server.key) {
        bridge.skip_to_latest().await.ok();
        bridge.refresh_topic().await;
    }
    ctx.say(format!(
        "Feed for **{}** set to <#{}>. Pause with `/chatbridge pause`.",
        server.label, channel.id
    ))
    .await?;
    Ok(())
}

#[poise::command(slash_command, required_permissions = "ADMINISTRATOR", guild_only)]
pub async fn status(
    ctx: Context<'_>,
    #[description = "Which server"]
    #[autocomplete = "autocomplete_server"]
    server: Option<String>,
) -> Result<(), Error> {
    let config = ctx.data().config.clone();
    let servers: Vec<_> = if let Some(ref key) = server {
        // A typo'd key used to silently filter down to zero rows, which
        // rendered as an empty table with no hint the key wasn't recognized.
        if resolve_server_checked(ctx, Some(key.as_str()), None)
            .await?
            .is_none()
        {
            return Ok(());
        }
        config
            .servers
            .iter()
            .filter(|s| s.key == *key)
            .cloned()
            .collect()
    } else {
        config.servers.clone()
    };
    let mut rows = Vec::new();
    for s in servers {
        match (
            ctx.data().db.get_bridge_settings(&s.key).await,
            ctx.data().bridges.get(&s.key),
        ) {
            (Ok(settings), Some(bridge)) => {
                let cursor = bridge.current_cursor().await;
                let behind = ctx
                    .data()
                    .db
                    .count_backlog(&s.key, cursor, None, &config.bridge.kinds)
                    .await
                    .unwrap_or(0);
                rows.push((
                    s.label,
                    settings.channel_id,
                    settings.enabled,
                    None,
                    Some(behind),
                ));
            }
            (Err(err), _) => rows.push((s.label, None, false, Some(err.to_string()), None)),
            _ => rows.push((s.label, None, false, Some("bridge missing".into()), None)),
        }
    }
    ctx.send(
        CreateReply::default()
            .embed(build_bridge_status_embed(&rows))
            .ephemeral(true),
    )
    .await?;
    Ok(())
}

async fn set_enabled(ctx: Context<'_>, server: Option<String>, enabled: bool) -> Result<(), Error> {
    let Some(server) = resolve_server_checked(ctx, server.as_deref(), None).await? else {
        return Ok(());
    };
    ctx.data()
        .db
        .set_bridge_enabled(&server.key, enabled)
        .await?;
    ctx.send(
        CreateReply::default()
            .content(format!(
                "Feed for **{}** is now {}.",
                server.label,
                if enabled { "resumed" } else { "paused" }
            ))
            .ephemeral(true),
    )
    .await?;
    Ok(())
}

#[poise::command(slash_command, required_permissions = "ADMINISTRATOR", guild_only)]
pub async fn pause(
    ctx: Context<'_>,
    #[description = "Which server"]
    #[autocomplete = "autocomplete_server"]
    server: Option<String>,
) -> Result<(), Error> {
    set_enabled(ctx, server, false).await
}

#[poise::command(slash_command, required_permissions = "ADMINISTRATOR", guild_only)]
pub async fn resume(
    ctx: Context<'_>,
    #[description = "Which server"]
    #[autocomplete = "autocomplete_server"]
    server: Option<String>,
) -> Result<(), Error> {
    set_enabled(ctx, server, true).await
}

#[poise::command(slash_command, required_permissions = "ADMINISTRATOR", guild_only)]
pub async fn topic(
    ctx: Context<'_>,
    #[description = "Which server"]
    #[autocomplete = "autocomplete_server"]
    server: Option<String>,
) -> Result<(), Error> {
    let Some(server) = resolve_server_checked(ctx, server.as_deref(), None).await? else {
        return Ok(());
    };
    let result = if let Some(bridge) = ctx.data().bridges.get(&server.key) {
        bridge.refresh_topic().await
    } else {
        TopicResult::Failed
    };
    let msg = match result {
        TopicResult::Updated => "Topic updated.",
        TopicResult::Unchanged => "Topic already up to date.",
        TopicResult::NoChannel => "No feed channel set.",
        TopicResult::Unsupported => "That channel type has no topic.",
        TopicResult::Forbidden => "Missing Manage Channel permission.",
        TopicResult::Failed => "Topic update failed.",
    };
    ctx.send(CreateReply::default().content(msg).ephemeral(true))
        .await?;
    Ok(())
}

#[poise::command(slash_command, required_permissions = "ADMINISTRATOR", guild_only)]
pub async fn test(
    ctx: Context<'_>,
    #[description = "Which server"]
    #[autocomplete = "autocomplete_server"]
    server: Option<String>,
) -> Result<(), Error> {
    let Some(server) = resolve_server_checked(ctx, server.as_deref(), None).await? else {
        return Ok(());
    };
    let ok = if let Some(bridge) = ctx.data().bridges.get(&server.key) {
        bridge.post_test_line().await.unwrap_or(false)
    } else {
        false
    };
    ctx.send(
        CreateReply::default()
            .content(if ok {
                "Posted a test line."
            } else {
                "Could not post — is the feed channel set?"
            })
            .ephemeral(true),
    )
    .await?;
    Ok(())
}

#[poise::command(slash_command, required_permissions = "ADMINISTRATOR", guild_only)]
/// Restore default event kinds and routes
pub async fn reset(
    ctx: Context<'_>,
    #[description = "Which server"]
    #[autocomplete = "autocomplete_server"]
    server: Option<String>,
) -> Result<(), Error> {
    let Some(server) = resolve_server_checked(ctx, server.as_deref(), None).await? else {
        return Ok(());
    };
    ctx.data()
        .db
        .reset_event_routes(&server.key, &ctx.data().config.bridge.kinds)
        .await?;
    ctx.data().db.set_bridge_enabled(&server.key, true).await?;
    let settings = ctx.data().db.get_bridge_settings(&server.key).await?;
    if let Some(bridge) = ctx.data().bridges.get(&server.key) {
        bridge.skip_to_latest().await.ok();
        bridge.refresh_topic().await;
    }
    let channel = settings
        .channel_id
        .map(|id| format!("<#{id}>"))
        .unwrap_or_else(|| "not set — run `/chatbridge set`".into());
    ctx.send(
        CreateReply::default()
            .content(format!(
                "Reset feed for **{}**: default event kinds, no per-kind channels, posting on. Channel: {channel}.",
                server.label
            ))
            .ephemeral(true),
    )
    .await?;
    Ok(())
}

#[poise::command(slash_command, required_permissions = "ADMINISTRATOR", guild_only)]
/// Disable the feed and clear its channel
pub async fn remove(
    ctx: Context<'_>,
    #[description = "Which server"]
    #[autocomplete = "autocomplete_server"]
    server: Option<String>,
) -> Result<(), Error> {
    let Some(server) = resolve_server_checked(ctx, server.as_deref(), None).await? else {
        return Ok(());
    };
    ctx.data().db.clear_bridge(&server.key).await?;
    if let Some(bridge) = ctx.data().bridges.get(&server.key) {
        bridge.skip_to_latest().await.ok();
        bridge.refresh_topic().await;
    }
    ctx.send(
        CreateReply::default()
            .content(format!(
                "Removed the feed for **{}**. Nothing will post until `/chatbridge set`.",
                server.label
            ))
            .ephemeral(true),
    )
    .await?;
    Ok(())
}

#[poise::command(slash_command, required_permissions = "ADMINISTRATOR", guild_only)]
/// Feed style, rainbow mode, and which events post where
pub async fn customize(
    ctx: Context<'_>,
    #[description = "Which server"]
    #[autocomplete = "autocomplete_server"]
    server: Option<String>,
) -> Result<(), Error> {
    use std::time::Duration;

    use futures::StreamExt;
    use serenity::builder::{CreateInteractionResponse, CreateInteractionResponseMessage};
    use serenity::collector::ComponentInteractionCollector;
    use serenity::model::application::ComponentInteractionDataKind;

    use crate::config::BridgeStyle;
    use crate::database::{canonical_route_kind, ROUTE_KINDS};
    use crate::presentation::ui::{bold, code};

    let Some(server) = resolve_server_checked(ctx, server.as_deref(), None).await? else {
        return Ok(());
    };
    ctx.data()
        .db
        .ensure_event_routes(&server.key, &ctx.data().config.bridge.kinds)
        .await?;
    let settings = ctx.data().db.get_bridge_settings(&server.key).await?;
    let Some(fallback) = settings.channel_id.clone() else {
        return send_embed(
            ctx,
            warning(
                "Set a main feed first",
                &format!(
                    "Run {} for {} so events have a default channel. Then come back here to split joins, deaths, etc.",
                    code("/chatbridge set"),
                    bold(&server.label)
                ),
            ),
        )
        .await;
    };

    let invoker = ctx.author().id;
    let key = server.key.clone();
    let label = server.label.clone();
    let db = ctx.data().db.clone();

    // The panel opens on a simple home view (style + rainbow + a way in to
    // routing) instead of dumping every event-routing control on screen at
    // once — "Edit routing" is the only door into the busier multi-select flow.
    let mut routing_view = false;
    let mut style = settings.style;
    let mut rainbow = settings.rainbow;
    let routes = db.get_event_routes(&key).await?;
    let reply = ctx
        .send(
            CreateReply::default()
                .embed(customize_embed(
                    &label,
                    &routes,
                    Some(&fallback),
                    style,
                    rainbow,
                ))
                .components(customize_panel_rows(
                    &key,
                    &routes,
                    None,
                    routing_view,
                    style,
                    rainbow,
                ))
                .ephemeral(true),
        )
        .await?;
    let msg = reply.message().await?;

    let mut collector = ComponentInteractionCollector::new(ctx.serenity_context())
        .message_id(msg.id)
        .timeout(Duration::from_secs(5 * 60))
        .stream();

    let mut pick_kind: Option<String> = None;
    while let Some(interaction) = collector.next().await {
        if interaction.user.id != invoker {
            interaction
                .create_response(
                    ctx.http(),
                    CreateInteractionResponse::Message(
                        CreateInteractionResponseMessage::new()
                            .content("Only the person who opened this panel can use it.")
                            .ephemeral(true),
                    ),
                )
                .await
                .ok();
            continue;
        }
        let id = interaction.data.custom_id.clone();
        match interaction.data.kind.clone() {
            ComponentInteractionDataKind::StringSelect { values }
                if id.starts_with("cb:style:") =>
            {
                style = BridgeStyle::parse(values.first().map(String::as_str).unwrap_or("rich"));
                db.set_bridge_style(&key, style).await?;
            }
            ComponentInteractionDataKind::Button if id.starts_with("cb:rainbow:") => {
                rainbow = !rainbow;
                db.set_bridge_rainbow(&key, rainbow).await?;
            }
            ComponentInteractionDataKind::Button if id.starts_with("cb:routing:") => {
                routing_view = true;
            }
            ComponentInteractionDataKind::Button if id.starts_with("cb:back:") => {
                routing_view = false;
                pick_kind = None;
            }
            ComponentInteractionDataKind::StringSelect { values }
                if id.starts_with("cb:toggle:") =>
            {
                let selected: std::collections::HashSet<_> = values.into_iter().collect();
                for kind in ROUTE_KINDS {
                    db.set_event_route_enabled(&key, kind, selected.contains(*kind))
                        .await?;
                }
                pick_kind = None;
            }
            ComponentInteractionDataKind::StringSelect { values } if id.starts_with("cb:pick:") => {
                pick_kind = values
                    .first()
                    .and_then(|v| canonical_route_kind(v))
                    .map(|s| s.to_string());
            }
            ComponentInteractionDataKind::ChannelSelect { values }
                if id.starts_with("cb:channel:") =>
            {
                let kind = id.split(':').nth(3).unwrap_or("");
                if canonical_route_kind(kind).is_some() {
                    let channel_id = values.first().map(|id| id.get().to_string());
                    db.set_event_route_channel(&key, kind, channel_id.as_deref())
                        .await?;
                    db.set_event_route_enabled(&key, kind, true).await?;
                }
                pick_kind = None;
            }
            _ => {}
        }

        let routes = db.get_event_routes(&key).await?;
        interaction
            .create_response(
                ctx.http(),
                CreateInteractionResponse::UpdateMessage(
                    CreateInteractionResponseMessage::new()
                        .embed(customize_embed(
                            &label,
                            &routes,
                            Some(&fallback),
                            style,
                            rainbow,
                        ))
                        .components(customize_panel_rows(
                            &key,
                            &routes,
                            pick_kind.as_deref(),
                            routing_view,
                            style,
                            rainbow,
                        )),
                ),
            )
            .await
            .ok();
    }

    reply
        .edit(ctx, CreateReply::default().components(Vec::new()))
        .await
        .ok();
    Ok(())
}

fn customize_embed(
    label: &str,
    routes: &[crate::database::EventRoute],
    fallback: Option<&str>,
    style: crate::config::BridgeStyle,
    rainbow: bool,
) -> poise::serenity_prelude::CreateEmbed {
    use crate::database::route_kind_label;
    use crate::presentation::ui::{skeleton, yes_no, Category};

    let summary = crate::database::ROUTE_KINDS
        .iter()
        .map(|kind| {
            let route = routes.iter().find(|r| r.kind == *kind);
            let on = route.map(|r| r.enabled).unwrap_or(false);
            let channel = route.and_then(|r| r.channel_id.as_deref()).or(fallback);
            let where_ = channel
                .map(|c| format!("<#{c}>"))
                .unwrap_or_else(|| "no channel".into());
            format!(
                "{} **{}** → {}",
                if on { "✓" } else { "·" },
                route_kind_label(kind),
                if on { where_ } else { "hidden".into() }
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    skeleton(Category::Admin)
        .title(format!("{label} event feed"))
        .description(format!(
            "`Style:` **{}**\n`Rainbow:` {}\n\n**Routing**\n{summary}\n\nUse **Edit routing** below to change which events post and where each one goes.",
            style.as_str(),
            yes_no(rainbow)
        ))
}

/// The panel's landing view: feed style, the rainbow toggle, and a door into
/// the busier per-event routing controls — not all of it at once.
fn customize_panel_rows(
    server_key: &str,
    routes: &[crate::database::EventRoute],
    pick_kind: Option<&str>,
    routing_view: bool,
    style: crate::config::BridgeStyle,
    rainbow: bool,
) -> Vec<poise::serenity_prelude::CreateActionRow> {
    if routing_view {
        routing_rows(server_key, routes, pick_kind)
    } else {
        home_rows(server_key, style, rainbow)
    }
}

fn home_rows(
    server_key: &str,
    style: crate::config::BridgeStyle,
    rainbow: bool,
) -> Vec<poise::serenity_prelude::CreateActionRow> {
    use serenity::builder::{
        CreateActionRow, CreateButton, CreateSelectMenu, CreateSelectMenuKind,
        CreateSelectMenuOption,
    };
    use serenity::ButtonStyle;

    use crate::config::BridgeStyle;

    let style_select = CreateSelectMenu::new(
        format!("cb:style:{server_key}"),
        CreateSelectMenuKind::String {
            options: vec![
                CreateSelectMenuOption::new("Rich", "rich")
                    .description("One embed per line, with player heads")
                    .default_selection(style == BridgeStyle::Rich),
                CreateSelectMenuOption::new("Compact", "compact")
                    .description("Batched lines, quieter on a busy server")
                    .default_selection(style == BridgeStyle::Compact),
            ],
        },
    )
    .placeholder("Feed style")
    .min_values(1)
    .max_values(1);

    vec![
        CreateActionRow::SelectMenu(style_select),
        CreateActionRow::Buttons(vec![
            CreateButton::new(format!("cb:rainbow:{server_key}"))
                .label(if rainbow {
                    "Rainbow: On"
                } else {
                    "Rainbow: Off"
                })
                .style(if rainbow {
                    ButtonStyle::Success
                } else {
                    ButtonStyle::Secondary
                }),
            CreateButton::new(format!("cb:routing:{server_key}"))
                .label("Edit routing")
                .style(ButtonStyle::Primary),
        ]),
    ]
}

/// The per-event controls: which kinds post, and an optional channel override
/// for each. One extra step behind "Edit routing" rather than always on screen.
fn routing_rows(
    server_key: &str,
    routes: &[crate::database::EventRoute],
    pick_kind: Option<&str>,
) -> Vec<poise::serenity_prelude::CreateActionRow> {
    use serenity::builder::{
        CreateActionRow, CreateButton, CreateSelectMenu, CreateSelectMenuKind,
        CreateSelectMenuOption,
    };
    use serenity::ButtonStyle;

    use crate::database::{route_kind_label, ROUTE_KINDS};

    let enabled: std::collections::HashSet<_> = routes
        .iter()
        .filter(|r| r.enabled)
        .map(|r| r.kind.clone())
        .collect();
    let toggle = CreateSelectMenu::new(
        format!("cb:toggle:{server_key}"),
        CreateSelectMenuKind::String {
            options: ROUTE_KINDS
                .iter()
                .map(|kind| {
                    CreateSelectMenuOption::new(route_kind_label(kind), *kind)
                        .description(if enabled.contains(*kind) {
                            "Currently shown"
                        } else {
                            "Currently hidden"
                        })
                        .default_selection(enabled.contains(*kind))
                })
                .collect(),
        },
    )
    .placeholder("Events to show (multi-select)")
    .min_values(0)
    .max_values(ROUTE_KINDS.len() as u8);

    let pick = CreateSelectMenu::new(
        format!("cb:pick:{server_key}"),
        CreateSelectMenuKind::String {
            options: ROUTE_KINDS
                .iter()
                .map(|kind| {
                    let custom = routes
                        .iter()
                        .find(|r| r.kind == *kind)
                        .and_then(|r| r.channel_id.as_ref())
                        .is_some();
                    CreateSelectMenuOption::new(
                        format!("Channel for {}", route_kind_label(kind)),
                        *kind,
                    )
                    .description(if custom {
                        "Custom channel set"
                    } else {
                        "Uses main feed channel"
                    })
                })
                .collect(),
        },
    )
    .placeholder("Pick an event to set its channel…")
    .min_values(1)
    .max_values(1);

    let mut rows = vec![
        CreateActionRow::SelectMenu(toggle),
        CreateActionRow::SelectMenu(pick),
    ];
    if let Some(kind) = pick_kind {
        rows.push(CreateActionRow::SelectMenu(
            CreateSelectMenu::new(
                format!("cb:channel:{server_key}:{kind}"),
                CreateSelectMenuKind::Channel {
                    channel_types: Some(vec![ChannelType::Text, ChannelType::News]),
                    default_channels: None,
                },
            )
            .placeholder(format!(
                "Channel for {}",
                crate::database::route_kind_label(kind)
            ))
            .min_values(0)
            .max_values(1),
        ));
    }
    rows.push(CreateActionRow::Buttons(vec![CreateButton::new(format!(
        "cb:back:{server_key}"
    ))
    .label("← Back")
    .style(ButtonStyle::Secondary)]));
    rows
}
