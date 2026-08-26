mod commands;
mod config;
mod database;
mod discord;
mod presentation;

use std::sync::Arc;
use std::time::{Duration, Instant};

use poise::serenity_prelude as serenity;
use serenity::GatewayIntents;

use config::Config;
use database::Db;
use discord::bridge::BridgeSet;
use presentation::ui;

pub type Error = eyre::Report;
pub type Context<'a> = poise::Context<'a, Data, Error>;

pub struct Data {
    pub config: Arc<Config>,
    pub db: Arc<Db>,
    pub bridges: Arc<BridgeSet>,
    pub started_at: Instant,
    /// Ready count; >0 means a reconnect, not startup.
    pub readys: std::sync::atomic::AtomicU32,
    /// Restricted command channel, 0 = any channel.
    pub command_channel: Arc<std::sync::atomic::AtomicU64>,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mc_discord_bot=info,warn".into()),
        )
        .init();

    let config = Arc::new(Config::load()?);
    let db = Arc::new(Db::connect(config.clone()).await?);

    let mut reachable = 0usize;
    for server in &config.servers {
        match db.ping_database(&server.key).await {
            Ok(_) => {
                if let Err(err) = db.ensure_bridge_state(&server.key).await {
                    tracing::error!("[db:{}] ensure schema failed: {err}", server.key);
                } else {
                    tracing::info!("[db:{}] connected ({})", server.key, server.label);
                    reachable += 1;
                }
            }
            Err(err) => tracing::error!("[db:{}] cannot reach database: {err}", server.key),
        }
    }
    if reachable == 0 {
        eyre::bail!("No database is reachable. Check SERVER_*_URL / DATABASE_URL.");
    }

    let command_channel = Arc::new(std::sync::atomic::AtomicU64::new(
        db.get_command_channel()
            .await?
            .and_then(|id| id.parse().ok())
            .unwrap_or(0),
    ));

    let token = config.discord.token.clone();
    let intents = GatewayIntents::GUILDS;

    let framework = poise::Framework::builder()
        .options(poise::FrameworkOptions {
            commands: commands::commands(),
            allowed_mentions: Some(
                serenity::builder::CreateAllowedMentions::new()
                    .all_users(false)
                    .all_roles(false)
                    .everyone(false),
            ),
            on_error: |error| {
                Box::pin(async move {
                    if let poise::FrameworkError::Command { error, ctx, .. } = error {
                        tracing::error!("[commands] /{} failed: {error:#}", ctx.command().name);
                        let embed = ui::failure(
                            "That did not work",
                            &format!(
                                "`/{}` could not finish. The database may be unreachable — try `/server status`.",
                                ctx.command().name
                            ),
                        );
                        let reply = poise::CreateReply::default().embed(embed).ephemeral(true);
                        if ctx.say("").await.is_err() {
                            let _ = ctx.send(reply).await;
                        } else {
                            let _ = ctx
                                .send(poise::CreateReply::default().embed(ui::failure(
                                    "That did not work",
                                    &format!(
                                        "`/{}` could not finish. Try `/server status`.",
                                        ctx.command().name
                                    ),
                                )))
                                .await;
                        }
                    } else if let poise::FrameworkError::GuildOnly { ctx, .. } = error {
                        // guild_only stops poise treating a DM as all-permissions-granted.
                        tracing::warn!(
                            "[commands] /{} refused: DM invocation is not allowed",
                            ctx.command().name
                        );
                        let _ = ctx
                            .send(
                                poise::CreateReply::default()
                                    .embed(ui::failure(
                                        "Server-only command",
                                        &format!("`/{}` only works in a server.", ctx.command().name),
                                    ))
                                    .ephemeral(true),
                            )
                            .await;
                    } else if let poise::FrameworkError::CommandCheckFailed { ctx, .. } = error {
                        let required = ctx
                            .data()
                            .command_channel
                            .load(std::sync::atomic::Ordering::Relaxed);
                        if required != 0 {
                            tracing::warn!(
                                "[commands] /{} refused: used outside <#{required}>",
                                ctx.command().name
                            );
                            let _ = ctx
                                .send(
                                    poise::CreateReply::default()
                                        .embed(ui::failure(
                                            "Wrong channel",
                                            &format!("Commands only work in <#{required}>."),
                                        ))
                                        .ephemeral(true),
                                )
                                .await;
                        }
                    } else if let poise::FrameworkError::MissingUserPermissions { ctx, .. } = error {
                        tracing::warn!(
                            "[commands] /{} refused: {} lacks Administrator",
                            ctx.command().name,
                            ctx.author().name
                        );
                        let _ = ctx
                            .send(
                                poise::CreateReply::default()
                                    .embed(ui::failure(
                                        "Admins only",
                                        &format!(
                                            "`/{}` needs the Administrator permission.",
                                            ctx.command().name
                                        ),
                                    ))
                                    .ephemeral(true),
                            )
                            .await;
                    } else {
                        tracing::error!("framework error: {error}");
                    }
                })
            },
            // Enforces /commandchannel; that command is exempt so admins can't lock themselves out.
            command_check: Some(|ctx| {
                Box::pin(async move {
                    if ctx.command().qualified_name.starts_with("commandchannel") {
                        return Ok(true);
                    }
                    let required = ctx
                        .data()
                        .command_channel
                        .load(std::sync::atomic::Ordering::Relaxed);
                    Ok(required == 0 || ctx.channel_id().get() == required)
                })
            }),
            event_handler: |_ctx, event, _framework, data| {
                Box::pin(async move {
                    match event {
                        serenity::FullEvent::Ready { data_about_bot } => {
                            let seen = data
                                .readys
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if seen > 0 {
                                tracing::warn!(
                                    "[discord] reconnected as {} — announcing to feed channels",
                                    data_about_bot.user.name
                                );
                                data.bridges.announce_bot_reconnect().await;
                            } else {
                                tracing::info!(
                                    "[discord] gateway ready as {}",
                                    data_about_bot.user.name
                                );
                            }
                        }
                        serenity::FullEvent::Resume { .. } => {
                            tracing::info!("[discord] gateway session resumed");
                        }
                        _ => {}
                    }
                    Ok(())
                })
            },
            ..Default::default()
        })
        .setup({
            let config = config.clone();
            let db = db.clone();
            let command_channel = command_channel.clone();
            move |ctx, ready, framework| {
                let config = config.clone();
                let db = db.clone();
                let command_channel = command_channel.clone();
                Box::pin(async move {
                    tracing::info!("[discord] logged in as {}", ready.user.name);
                    tracing::info!(
                        "[discord] serving {} server(s): {}",
                        config.servers.len(),
                        config
                            .servers
                            .iter()
                            .map(|s| s.label.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    );

                    if let Some(guild_id) = config.discord.guild_id {
                        poise::builtins::register_in_guild(
                            ctx,
                            &framework.options().commands,
                            serenity::GuildId::new(guild_id),
                        )
                        .await?;
                        tracing::info!(
                            "[commands] registered {} commands to guild {guild_id}",
                            framework.options().commands.len()
                        );
                    } else {
                        poise::builtins::register_globally(ctx, &framework.options().commands)
                            .await?;
                        tracing::info!(
                            "[commands] registered {} commands globally (may take up to an hour)",
                            framework.options().commands.len()
                        );
                    }

                    let bridges = Arc::new(BridgeSet::new(
                        ctx.http.clone(),
                        config.clone(),
                        db.clone(),
                    ));
                    let bridges_start = bridges.clone();
                    tokio::spawn(async move {
                        bridges_start.start_all().await;
                    });

                    let presence_db = db.clone();
                    let presence_config = config.clone();
                    let presence_ctx = ctx.clone();
                    tokio::spawn(async move {
                        let mut interval = tokio::time::interval(Duration::from_secs(15));
                        loop {
                            interval.tick().await;
                            let mut online = 0i64;
                            for server in &presence_config.servers {
                                online += presence_db.count_online(&server.key).await.unwrap_or(0);
                            }
                            use serenity::gateway::ActivityData;
                            presence_ctx.set_presence(
                                Some(ActivityData::watching(format!(
                                    "{online} players · /help"
                                ))),
                                serenity::OnlineStatus::Online,
                            );
                        }
                    });

                    Ok(Data {
                        config,
                        db,
                        bridges,
                        started_at: Instant::now(),
                        readys: std::sync::atomic::AtomicU32::new(0),
                        command_channel,
                    })
                })
            }
        })
        .build();

    let mut client = serenity::Client::builder(&token, intents)
        .framework(framework)
        .await?;

    let shard_manager = client.shard_manager.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("[main] SIGINT — shutting down");
        shard_manager.shutdown_all().await;
    });

    client.start().await?;
    Ok(())
}
