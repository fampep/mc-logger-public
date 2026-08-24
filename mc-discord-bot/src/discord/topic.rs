use std::sync::Arc;
use std::time::{Duration, Instant};

use poise::serenity_prelude as serenity;
use serenity::builder::EditChannel;
use serenity::model::id::ChannelId;

use crate::config::{Config, ServerConfig};
use crate::database::Db;
use crate::presentation::ui::{clamp, fmt, Limits};

const SEPARATOR: &str = " · ";

fn minutes_since(date: chrono::DateTime<chrono::Utc>) -> i64 {
    ((chrono::Utc::now() - date).num_seconds() / 60).max(0)
}

fn since(date: Option<chrono::DateTime<chrono::Utc>>) -> String {
    let Some(date) = date else {
        return "no chat logged yet".into();
    };
    let minutes = minutes_since(date);
    if minutes < 1 {
        return "last line just now".into();
    }
    if minutes < 60 {
        return format!("last line {minutes}m ago");
    }
    let hours = minutes / 60;
    if hours < 24 {
        format!("last line {hours}h ago")
    } else {
        format!("last line {}d ago", hours / 24)
    }
}

fn stamp(at: chrono::DateTime<chrono::Utc>) -> String {
    format!("updated {:02}:{:02} UTC", at.hour(), at.minute())
}

use chrono::Timelike;

pub fn render_topic(
    server: &ServerConfig,
    stats: &crate::database::TopicStats,
    at: chrono::DateTime<chrono::Utc>,
) -> String {
    let mut parts = vec![stats.host.clone().unwrap_or_else(|| server.label.clone())];
    if stats.logger_live {
        parts.push(format!("{} online", fmt(stats.online as f64)));
        if stats.peak_today > stats.online {
            parts.push(format!("peak {} today", fmt(stats.peak_today as f64)));
        }
    } else {
        parts.push("logger offline".into());
        parts.push(since(stats.last_message_at));
    }
    if stats.players_24h > 0 {
        parts.push(format!("{} players in 24h", fmt(stats.players_24h as f64)));
    }
    if stats.joins_1h > 0 {
        parts.push(format!("{} joins/h", fmt(stats.joins_1h as f64)));
    }
    if stats.chat_1h > 0 {
        parts.push(format!("{} chat/h", fmt(stats.chat_1h as f64)));
    }
    if stats.deaths_24h > 0 {
        parts.push(format!("{} deaths in 24h", fmt(stats.deaths_24h as f64)));
    }
    parts.push(stamp(at));
    clamp(&parts.join(SEPARATOR), Limits::TOPIC)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicResult {
    Updated,
    Unchanged,
    NoChannel,
    Unsupported,
    Forbidden,
    Failed,
}

pub struct ChannelTopic {
    http: Arc<serenity::http::Http>,
    server: ServerConfig,
    config: Arc<Config>,
    db: Arc<Db>,
    last_text: Option<String>,
    last_run_at: Instant,
    complained_about: Option<String>,
}

impl ChannelTopic {
    pub fn new(
        http: Arc<serenity::http::Http>,
        server: ServerConfig,
        config: Arc<Config>,
        db: Arc<Db>,
    ) -> Self {
        Self {
            http,
            server,
            config,
            db,
            last_text: None,
            last_run_at: Instant::now()
                .checked_sub(Duration::from_secs(10_000))
                .unwrap_or_else(Instant::now),
            complained_about: None,
        }
    }

    pub async fn maybe_update(&mut self, channel_id: Option<&str>) -> TopicResult {
        if !self.config.topic.enabled {
            return TopicResult::Unchanged;
        }
        if self.last_run_at.elapsed() < Duration::from_millis(self.config.topic.interval_ms) {
            return TopicResult::Unchanged;
        }
        self.update(channel_id).await
    }

    pub async fn update(&mut self, channel_id: Option<&str>) -> TopicResult {
        self.last_run_at = Instant::now();
        let Some(channel_id) = channel_id else {
            return TopicResult::NoChannel;
        };
        let Ok(id) = channel_id.parse::<u64>() else {
            return TopicResult::Unsupported;
        };
        let channel_id = ChannelId::new(id);

        let text = match self.build_text().await {
            Ok(t) => t,
            Err(err) => {
                tracing::error!("[topic:{}] stats failed: {err}", self.server.key);
                return TopicResult::Failed;
            }
        };

        if self.last_text.as_deref() == Some(&text) {
            return TopicResult::Unchanged;
        }

        match channel_id
            .edit(&self.http, EditChannel::new().topic(&text))
            .await
        {
            Ok(_) => {
                self.last_text = Some(text);
                self.complained_about = None;
                TopicResult::Updated
            }
            Err(err) => {
                let msg = err.to_string();
                if msg.contains("Missing Access")
                    || msg.contains("Missing Permissions")
                    || msg.contains("50013")
                    || msg.contains("50001")
                {
                    if self.complained_about.as_deref() != Some(channel_id.to_string().as_str()) {
                        self.complained_about = Some(channel_id.to_string());
                        tracing::error!(
                            "[topic:{}] cannot edit channel: the bot needs Manage Channel there. Grant it or set TOPIC_UPDATES=false.",
                            self.server.key
                        );
                    }
                    TopicResult::Forbidden
                } else {
                    tracing::error!("[topic:{}] update failed: {msg}", self.server.key);
                    TopicResult::Failed
                }
            }
        }
    }

    async fn build_text(&self) -> eyre::Result<String> {
        let mut stats = self.db.topic_stats(&self.server.key).await?;
        stats.peak_today = self
            .db
            .record_online_peak(&self.server.key, stats.online)
            .await?;
        Ok(render_topic(&self.server, &stats, chrono::Utc::now()))
    }
}
