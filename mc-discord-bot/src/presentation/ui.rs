//! Shared Discord formatting and interactive UI helpers.

use std::future::Future;
use std::time::Duration;

use futures::StreamExt;

use poise::serenity_prelude as serenity;
use poise::CreateReply;
use serenity::builder::{
    CreateActionRow, CreateButton, CreateEmbed, CreateInteractionResponse,
    CreateInteractionResponseMessage, CreateSelectMenu, CreateSelectMenuKind,
    CreateSelectMenuOption,
};
use serenity::collector::ComponentInteractionCollector;
use serenity::model::application::ComponentInteraction;
use serenity::model::channel::ReactionType;
use serenity::{ButtonStyle, ComponentInteractionDataKind};

use crate::{Context, Error};

pub use super::formatting::*;

const PAGER_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Select options for the `/stats`-style pager (one menu, not a row of tab buttons).
#[derive(Clone, Copy)]
pub struct NavSelect {
    pub custom_id: &'static str,
    pub placeholder: &'static str,
    pub options: &'static [(&'static str, usize)],
}

pub const RANGE_MENU: &[(&str, usize)] = &[
    ("7 days", 0),
    ("30 days", 1),
    ("60 days", 2),
    ("90 days", 3),
    ("All time", 4),
];

pub const TIMEFRAME_SELECT: NavSelect = NavSelect {
    custom_id: "nav:range",
    placeholder: "Timeframe",
    options: RANGE_MENU,
};

pub const BOARD_MENU: &[(&str, usize)] = &[
    ("PvP kills", 0),
    ("K/D", 1),
    ("Deaths", 2),
    ("Messages", 3),
    ("Joins", 4),
    ("Playtime", 5),
];

pub const BOARD_SELECT: NavSelect = NavSelect {
    custom_id: "nav:board",
    placeholder: "Board",
    options: BOARD_MENU,
};

pub struct NavPage {
    pub embed: CreateEmbed,
    pub page_count: usize,
}

impl NavPage {
    pub fn one(embed: CreateEmbed) -> Self {
        Self {
            embed,
            page_count: 1,
        }
    }
}

fn stats_nav_row(page: usize, page_count: usize) -> CreateActionRow {
    let first = page == 0;
    let last = page + 1 >= page_count.max(1);
    CreateActionRow::Buttons(vec![
        CreateButton::new("page:first")
            .label("<<")
            .style(ButtonStyle::Success)
            .disabled(first),
        CreateButton::new("page:prev")
            .label("<")
            .style(ButtonStyle::Primary)
            .disabled(first),
        CreateButton::new("page:next")
            .label(">")
            .style(ButtonStyle::Primary)
            .disabled(last),
        CreateButton::new("page:last")
            .label(">>")
            .style(ButtonStyle::Success)
            .disabled(last),
        CreateButton::new("page:close")
            .label("X")
            .style(ButtonStyle::Danger),
    ])
}

fn stats_select_row(select: &NavSelect, selected: usize) -> CreateActionRow {
    CreateActionRow::SelectMenu(
        CreateSelectMenu::new(
            select.custom_id,
            CreateSelectMenuKind::String {
                options: select
                    .options
                    .iter()
                    .map(|(label, idx)| {
                        CreateSelectMenuOption::new(*label, idx.to_string())
                            .default_selection(*idx == selected)
                    })
                    .collect(),
            },
        )
        .placeholder(select.placeholder)
        .min_values(1)
        .max_values(1),
    )
}

fn nav_pager_rows(
    page: usize,
    page_count: usize,
    select: Option<&NavSelect>,
    select_idx: usize,
) -> Vec<CreateActionRow> {
    let mut rows = Vec::new();
    if let Some(select) = select {
        rows.push(stats_select_row(select, select_idx));
    }
    rows.push(stats_nav_row(page, page_count));
    rows
}

/// Player `/stats` pager: optional select + `<<` `<` `>` `>>` `X`.
pub async fn send_nav_pager<R, RF>(
    ctx: Context<'_>,
    start_page: usize,
    select: Option<&'static NavSelect>,
    start_select: usize,
    mut render: R,
) -> Result<(), Error>
where
    R: FnMut(usize, usize) -> RF,
    RF: Future<Output = Result<NavPage, Error>>,
{
    let _ = ctx.defer().await;
    let invoker = ctx.author().id;
    let command_name = ctx.command().name.clone();
    let select_count = select.map(|s| s.options.len()).unwrap_or(0);
    let mut select_idx = if select_count == 0 {
        0
    } else {
        start_select.min(select_count.saturating_sub(1))
    };
    let mut page = start_page;

    let rendered = render(page, select_idx).await?;
    let mut page_count = rendered.page_count.max(1);
    page = page.min(page_count.saturating_sub(1));
    let handle = ctx
        .send(
            CreateReply::default()
                .embed(rendered.embed)
                .components(nav_pager_rows(page, page_count, select, select_idx)),
        )
        .await?;
    let msg = handle.message().await?;

    let mut collector = ComponentInteractionCollector::new(ctx.serenity_context())
        .message_id(msg.id)
        .timeout(PAGER_TIMEOUT)
        .stream();

    while let Some(interaction) = collector.next().await {
        if interaction.user.id != invoker {
            interaction
                .create_response(
                    ctx.http(),
                    CreateInteractionResponse::Message(
                        CreateInteractionResponseMessage::new()
                            .content(format!(
                                "Only {} can page this one. Run `/{command_name}` to get your own.",
                                ctx.author().name
                            ))
                            .ephemeral(true),
                    ),
                )
                .await
                .ok();
            continue;
        }

        let id = interaction.data.custom_id.clone();
        let mut closed = false;
        match &interaction.data.kind {
            ComponentInteractionDataKind::StringSelect { values } => {
                if let Some(idx) = values
                    .first()
                    .and_then(|v| v.parse::<usize>().ok())
                    .filter(|&i| select_count == 0 || i < select_count)
                {
                    if idx != select_idx {
                        select_idx = idx;
                        page = 0;
                    }
                }
            }
            ComponentInteractionDataKind::Button if id == "page:close" => {
                closed = true;
            }
            ComponentInteractionDataKind::Button => {
                page = match id.as_str() {
                    "page:first" => 0,
                    "page:prev" => page.saturating_sub(1),
                    "page:next" => (page + 1).min(page_count.saturating_sub(1)),
                    "page:last" => page_count.saturating_sub(1),
                    _ => page,
                };
            }
            _ => {}
        }

        if closed {
            interaction
                .create_response(
                    ctx.http(),
                    CreateInteractionResponse::UpdateMessage(
                        CreateInteractionResponseMessage::new().components(Vec::new()),
                    ),
                )
                .await
                .ok();
            return Ok(());
        }

        match render(page, select_idx).await {
            Ok(rendered) => {
                page_count = rendered.page_count.max(1);
                page = page.min(page_count.saturating_sub(1));
                interaction
                    .create_response(
                        ctx.http(),
                        CreateInteractionResponse::UpdateMessage(
                            CreateInteractionResponseMessage::new()
                                .embed(rendered.embed)
                                .components(nav_pager_rows(page, page_count, select, select_idx)),
                        ),
                    )
                    .await
                    .ok();
            }
            Err(err) => {
                tracing::error!("[ui] nav pager failed on /{command_name}: {err:#}");
            }
        }
    }

    handle
        .edit(
            ctx,
            CreateReply::default().components(Vec::<CreateActionRow>::new()),
        )
        .await
        .ok();
    Ok(())
}

fn nav_button(id: &str, label: &str, disabled: bool) -> CreateButton {
    CreateButton::new(id)
        .label(label)
        .style(ButtonStyle::Secondary)
        .disabled(disabled)
}

fn nav_row(prefix: &str, index: usize, count: usize) -> CreateActionRow {
    CreateActionRow::Buttons(vec![
        nav_button(&format!("{prefix}:first"), "First", index == 0),
        nav_button(&format!("{prefix}:prev"), "Previous", index == 0),
        CreateButton::new(format!("{prefix}:indicator"))
            .label(format!("{} / {}", index + 1, count))
            .style(ButtonStyle::Secondary)
            .disabled(true),
        nav_button(&format!("{prefix}:next"), "Next", index + 1 >= count),
        nav_button(&format!("{prefix}:last"), "Last", index + 1 >= count),
    ])
}

#[allow(clippy::too_many_arguments)] // Internal state renderer for Discord component rows.
fn pager_rows(
    page: usize,
    sub_page: usize,
    sub_page_count: usize,
    page_count: usize,
    tabs: Option<&[&str]>,
    refresh: bool,
    ranges: Option<&[&str]>,
    range_idx: usize,
) -> Vec<CreateActionRow> {
    let mut rows = Vec::new();
    let mut last_tab_len = 0usize;
    let mut refresh_placed = false;

    if let Some(tabs) = tabs {
        for start in (0..tabs.len()).step_by(5) {
            let end = (start + 5).min(tabs.len());
            let buttons: Vec<_> = tabs[start..end]
                .iter()
                .enumerate()
                .map(|(offset, label)| {
                    let index = start + offset;
                    CreateButton::new(format!("page:{index}"))
                        .label(*label)
                        .style(if index == page {
                            ButtonStyle::Primary
                        } else {
                            ButtonStyle::Secondary
                        })
                        .disabled(index == page)
                })
                .collect();
            last_tab_len = buttons.len();
            rows.push(CreateActionRow::Buttons(buttons));
        }
    } else if page_count > 1 {
        rows.push(nav_row("page", page, page_count));
    }

    if let Some(ranges) = ranges {
        let mut buttons: Vec<_> = ranges
            .iter()
            .enumerate()
            .map(|(index, label)| {
                CreateButton::new(format!("range:{index}"))
                    .label(*label)
                    .style(if index == range_idx {
                        ButtonStyle::Primary
                    } else {
                        ButtonStyle::Secondary
                    })
                    .disabled(index == range_idx)
            })
            .collect();
        if refresh && buttons.len() < 5 {
            buttons.push(
                CreateButton::new("page:refresh")
                    .label("Refresh")
                    .style(ButtonStyle::Secondary),
            );
            refresh_placed = true;
        }
        rows.push(CreateActionRow::Buttons(buttons));
    }

    if tabs.is_some() && sub_page_count > 1 {
        rows.push(nav_row("sub", sub_page, sub_page_count));
    }

    if refresh && !refresh_placed {
        let refresh_btn = CreateButton::new("page:refresh")
            .label("Refresh")
            .style(ButtonStyle::Secondary);
        if let Some(CreateActionRow::Buttons(ref mut buttons)) = rows.last_mut() {
            if last_tab_len > 0 && last_tab_len < 5 && ranges.is_none() {
                buttons.push(refresh_btn);
            } else {
                rows.push(CreateActionRow::Buttons(vec![refresh_btn]));
            }
        } else {
            rows.push(CreateActionRow::Buttons(vec![refresh_btn]));
        }
    }
    rows
}

fn next_index(custom_id: &str, index: usize, count: usize) -> usize {
    let action = custom_id.split(':').nth(1).unwrap_or("");
    match action {
        "first" => 0,
        "prev" => index.saturating_sub(1),
        "next" => (index + 1).min(count.saturating_sub(1)),
        "last" => count.saturating_sub(1),
        "refresh" => index,
        other => other
            .parse::<usize>()
            .ok()
            .filter(|&t| t < count)
            .unwrap_or(index),
    }
}

pub async fn send_embed(ctx: Context<'_>, embed: CreateEmbed) -> Result<(), Error> {
    ctx.send(CreateReply::default().embed(embed)).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)] // Stable command-facing pager API.
pub async fn send_paged<F, Fut, R>(
    ctx: Context<'_>,
    page_count: usize,
    tabs: Option<&[&str]>,
    refresh: bool,
    start_page: usize,
    sub_page_count: R,
    on_refresh: impl FnMut(),
    render: F,
) -> Result<(), Error>
where
    F: FnMut(usize, usize) -> Fut,
    Fut: Future<Output = Result<CreateEmbed, Error>>,
    R: FnMut(usize) -> std::pin::Pin<Box<dyn Future<Output = Result<usize, Error>> + Send>>,
{
    paged(
        ctx,
        page_count,
        tabs,
        refresh,
        start_page,
        None,
        0,
        |_| {},
        sub_page_count,
        on_refresh,
        render,
    )
    .await
}

#[allow(clippy::too_many_arguments)] // Internal pager state machine inputs are independent.
async fn paged<F, Fut, R>(
    ctx: Context<'_>,
    page_count: usize,
    tabs: Option<&[&str]>,
    refresh: bool,
    start_page: usize,
    ranges: Option<&[&str]>,
    start_range: usize,
    mut on_range: impl FnMut(usize),
    mut sub_page_count: R,
    mut on_refresh: impl FnMut(),
    mut render: F,
) -> Result<(), Error>
where
    F: FnMut(usize, usize) -> Fut,
    Fut: Future<Output = Result<CreateEmbed, Error>>,
    R: FnMut(usize) -> std::pin::Pin<Box<dyn Future<Output = Result<usize, Error>> + Send>>,
{
    let _ = ctx.defer().await;
    let invoker = ctx.author().id;
    let command_name = ctx.command().name.clone();
    let range_count = ranges.map(|r| r.len()).unwrap_or(0);
    let mut range_idx = if range_count == 0 {
        0
    } else {
        start_range.min(range_count.saturating_sub(1))
    };

    let mut page = start_page.min(page_count.saturating_sub(1));
    let mut sub_page = 0usize;
    let mut sub_count = sub_page_count(page).await?.max(1);

    let components = |page: usize, sub_page: usize, sub_count: usize, range_idx: usize| {
        pager_rows(
            page, sub_page, sub_count, page_count, tabs, refresh, ranges, range_idx,
        )
    };

    let embed = render(page, sub_page).await?;
    let handle = ctx
        .send(
            CreateReply::default()
                .embed(embed)
                .components(components(page, sub_page, sub_count, range_idx)),
        )
        .await?;
    let msg = handle.message().await?;

    let interactive =
        tabs.is_some() || page_count > 1 || refresh || sub_count > 1 || ranges.is_some();
    if !interactive {
        return Ok(());
    }

    let mut collector = ComponentInteractionCollector::new(ctx.serenity_context())
        .message_id(msg.id)
        .timeout(PAGER_TIMEOUT)
        .filter(move |i| matches!(i.data.kind, ComponentInteractionDataKind::Button))
        .stream();

    while let Some(interaction) = collector.next().await {
        if interaction.user.id != invoker {
            interaction
                .create_response(
                    ctx.http(),
                    CreateInteractionResponse::Message(
                        CreateInteractionResponseMessage::new()
                            .content(format!(
                                "Only {} can page this one. Run `/{command_name}` to get your own.",
                                ctx.author().name
                            ))
                            .ephemeral(true),
                    ),
                )
                .await
                .ok();
            continue;
        }

        let id = interaction.data.custom_id.clone();
        if id.starts_with("sub:") {
            sub_page = next_index(&id, sub_page, sub_count);
        } else if id.starts_with("range:") {
            let idx = id
                .split(':')
                .nth(1)
                .and_then(|s| s.parse::<usize>().ok())
                .filter(|&i| i < range_count)
                .unwrap_or(range_idx);
            if idx != range_idx {
                range_idx = idx;
                on_range(idx);
                on_refresh();
                sub_page = 0;
                sub_count = sub_page_count(page).await?.max(1);
            }
        } else {
            let target = next_index(&id, page, page_count);
            if target != page || id == "page:refresh" {
                if id == "page:refresh" {
                    on_refresh();
                }
                page = target;
                sub_page = 0;
                sub_count = sub_page_count(page).await?.max(1);
            }
        }

        match render(page, sub_page).await {
            Ok(embed) => {
                interaction
                    .create_response(
                        ctx.http(),
                        CreateInteractionResponse::UpdateMessage(
                            CreateInteractionResponseMessage::new()
                                .embed(embed)
                                .components(components(page, sub_page, sub_count, range_idx)),
                        ),
                    )
                    .await
                    .ok();
            }
            Err(err) => {
                tracing::error!("[ui] pager update failed on /{command_name}: {err:#}");
            }
        }
    }

    handle
        .edit(
            ctx,
            CreateReply::default().components(Vec::<CreateActionRow>::new()),
        )
        .await
        .ok();
    Ok(())
}

pub async fn send_choices<F, Fut>(
    ctx: Context<'_>,
    embed: CreateEmbed,
    choices: &[(String, String)],
    on_pick: F,
) -> Result<(), Error>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = Result<(), Error>>,
{
    let _ = ctx.defer().await;
    let invoker = ctx.author().id;
    let command_name = ctx.command().name.clone();

    let mut rows = Vec::new();
    for chunk in choices.iter().take(25).collect::<Vec<_>>().chunks(5) {
        let buttons: Vec<_> = chunk
            .iter()
            .map(|(id, label)| {
                CreateButton::new(format!("pick:{id}"))
                    .label(clamp(label, 80))
                    .style(ButtonStyle::Primary)
            })
            .collect();
        rows.push(CreateActionRow::Buttons(buttons));
    }

    let handle = ctx
        .send(CreateReply::default().embed(embed).components(rows))
        .await?;
    let msg = handle.message().await?;

    let mut collector = ComponentInteractionCollector::new(ctx.serenity_context())
        .message_id(msg.id)
        .timeout(PAGER_TIMEOUT)
        .stream();

    while let Some(interaction) = collector.next().await {
        if interaction.user.id != invoker {
            interaction
                .create_response(
                    ctx.http(),
                    CreateInteractionResponse::Message(
                        CreateInteractionResponseMessage::new()
                            .content(format!(
                                "Only {} can pick on this one. Run `/{command_name}` to get your own.",
                                ctx.author().name
                            ))
                            .ephemeral(true),
                    ),
                )
                .await
                .ok();
            continue;
        }
        let id = interaction
            .data
            .custom_id
            .strip_prefix("pick:")
            .unwrap_or("")
            .to_string();
        interaction
            .create_response(ctx.http(), CreateInteractionResponse::Acknowledge)
            .await
            .ok();
        return on_pick(id).await;
    }

    handle
        .edit(
            ctx,
            CreateReply::default().components(Vec::<CreateActionRow>::new()),
        )
        .await
        .ok();
    Ok(())
}

#[allow(dead_code)]
fn _unused_reaction(_: ReactionType, _: ComponentInteraction) {}
