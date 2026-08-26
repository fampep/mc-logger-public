pub mod chat;
pub mod chatbridge;
pub mod command_channel;
pub mod database;
pub mod help;
pub mod keyword;
pub mod leaderboard;
pub mod message_log;
pub mod shared;
pub mod stats;

use crate::{Data, Error};

pub fn commands() -> Vec<poise::Command<Data, Error>> {
    vec![
        help::help(),
        stats::stats(),
        keyword::keyword(),
        chat::chat(),
        leaderboard::leaderboard(),
        chatbridge::chatbridge(),
        database::database(),
        command_channel::commandchannel(),
    ]
}
