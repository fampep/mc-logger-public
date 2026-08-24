//! Re-runs the classifier over rows already in the database.
//!
//! A template is almost always added long after the lines it matches were
//! logged, so a death that is recognised today was stored as `server` when it
//! arrived and never reached anybody's stats. Kills are worse: a line that did
//! match a vanilla template could still store the wrong killer — 2b2t's
//! "X was blown up by Y with an end crystal" matched vanilla's "%1$s was blown
//! up by %2$s" and saved the killer as "Y with an end crystal", which is not a
//! name any player has, so `player_kills` dropped it.
//!
//! `discover` runs this automatically after adding a template. This binary is
//! the same pass run by hand, with a report of what it did.
//!
//!   cargo run --bin reclassify                    # dry run, prints what would change
//!   cargo run --bin reclassify -- --apply         # writes
//!   ENV_FILE=.env.2b2t cargo run --bin reclassify -- --apply
//!
//! Dry run is the default because this rewrites history in place.

// Only the death matchers are used here, so the chat side of the module is
// dead code in this binary specifically.
#[allow(dead_code)]
#[path = "../classify.rs"]
mod classify;

#[path = "../backfill.rs"]
mod backfill;

use std::collections::{HashMap, HashSet};

use backfill::{Change, Scan};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    load_env();

    let apply = std::env::args().any(|arg| arg == "--apply");

    let database_url = match std::env::var("DATABASE_URL") {
        Ok(url) if !url.trim().is_empty() => url,
        _ => {
            eprintln!("Set DATABASE_URL, or ENV_FILE to a file that defines it.");
            std::process::exit(1);
        }
    };

    let (mut client, connection) =
        tokio_postgres::connect(&database_url, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("connection lost: {error}");
        }
    });

    println!("{} death patterns loaded", classify::death_matcher_count());

    // Opt-in because it walks every chat row rather than the two buckets a
    // death can hide in, and only ever needs running once per server.
    if std::env::args().any(|arg| arg == "--prefixes") {
        let filled = backfill::backfill_prefixes(&mut client).await?;
        println!("filled sender_prefix on {filled} chat row(s)");
    }

    let scan = backfill::scan(&client).await?;

    // Kills only count when the killer is a known player — that is what keeps
    // mobs out of the leaderboard — so it is worth reporting separately how
    // many of the corrections actually reach somebody's stats.
    let known_players: HashSet<String> = client
        .query("SELECT lower(name) AS name FROM players", &[])
        .await?
        .iter()
        .map(|row| row.get::<_, String>("name"))
        .collect();

    report(&scan, &known_players);

    if scan.changes.is_empty() {
        println!("\nNothing to do.");
        return Ok(());
    }

    if !apply {
        println!("\nDry run — nothing written. Re-run with --apply to write these.");
        return Ok(());
    }

    let written = backfill::apply(&mut client, &scan.changes).await?;
    println!("\nApplied {written} row(s).");
    Ok(())
}

fn report(scan: &Scan, known_players: &HashSet<String>) {
    // Rescues split by what the row turned out to be, or a server whose chat
    // format was taught late reads as though it had 800 unrecognised deaths.
    let deaths_rescued = scan
        .changes
        .iter()
        .filter(|c| c.is_rescue() && c.kind == "death")
        .count();
    let chat_rescued = scan.changes.iter().filter(|c| c.kind == "chat").count();
    let corrected = scan.changes.len() - deaths_rescued - chat_rescued;

    println!(
        "\nscanned {} row(s): {deaths_rescued} unrecognised death(s), \
         {chat_rescued} unrecognised chat message(s), \
         {corrected} death(s) with the wrong names, {} already correct, \
         {} unclassified",
        scan.scanned, scan.unchanged, scan.not_a_death
    );

    if !scan.downgrades.is_empty() {
        println!(
            "\n{} row(s) stored as deaths no longer match any template — left alone:",
            scan.downgrades.len()
        );
        for text in scan.downgrades.iter().take(10) {
            println!("  {text}");
        }
    }

    if scan.changes.is_empty() {
        return;
    }

    println!("\nDeaths now recognised:");
    for change in scan.changes.iter().filter(|c| c.is_rescue() && c.kind == "death").take(30) {
        println!(
            "  [{}] {}  ->  victim {}, killer {}",
            change.was_kind,
            change.plain_text,
            change.subject.as_deref().unwrap_or("-"),
            change.killer.as_deref().unwrap_or("-"),
        );
    }

    let chat: Vec<&Change> = scan.changes.iter().filter(|c| c.kind == "chat").collect();
    if !chat.is_empty() {
        println!("\nChat now recognised (was filed as a server notice):");
        for change in chat.iter().take(15) {
            println!(
                "  {}  ->  {} said {:?}",
                change.plain_text,
                change.sender.as_deref().unwrap_or("-"),
                change.content.as_deref().unwrap_or(""),
            );
        }
        if chat.len() > 15 {
            println!("  ... and {} more", chat.len() - 15);
        }
    }

    let corrections: Vec<&Change> = scan.changes.iter().filter(|c| !c.is_rescue()).collect();
    if !corrections.is_empty() {
        println!("\nDeaths whose victim or killer was wrong:");
        for change in corrections.iter().take(30) {
            println!(
                "  {}\n    victim {} -> {}, killer {} -> {}",
                change.plain_text,
                change.was_subject.as_deref().unwrap_or("-"),
                change.subject.as_deref().unwrap_or("-"),
                change.was_killer.as_deref().unwrap_or("-"),
                change.killer.as_deref().unwrap_or("-"),
            );
        }
    }

    // What this actually does to the leaderboard. A killer that is a mob, or a
    // player the bot never saw in the tab list, is not in `players` and so is
    // filtered out of `player_kills` however correct the name is.
    let mut gained: HashMap<String, usize> = HashMap::new();
    let mut lost_to_unknown = 0usize;
    for change in &scan.changes {
        let Some(killer) = change.killer.as_deref() else { continue };
        let counted_before = change
            .was_killer
            .as_deref()
            .is_some_and(|name| known_players.contains(&name.to_lowercase()));
        if counted_before {
            continue;
        }
        if known_players.contains(&killer.to_lowercase()) {
            *gained.entry(killer.to_owned()).or_default() += 1;
        } else {
            lost_to_unknown += 1;
        }
    }

    if !gained.is_empty() {
        let mut tally: Vec<(&String, &usize)> = gained.iter().collect();
        tally.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
        println!("\nPvP kills these corrections add to the leaderboard:");
        for (name, count) in tally {
            println!("  {count:>3}  {name}");
        }
    }

    let deaths_gained = scan
        .changes
        .iter()
        .filter(|c| c.is_rescue() && c.subject.is_some())
        .count();
    println!("\n{deaths_gained} death(s) will be added to victims' stats.");

    if lost_to_unknown > 0 {
        println!(
            "{lost_to_unknown} corrected killer(s) are not in `players` — mobs, or players the \
             bot never saw in the tab list. Those stay out of the leaderboard by design."
        );
    }
}

/// Matches the logger's own convention, so the same ENV_FILE selects the same
/// database.
fn load_env() {
    match std::env::var("ENV_FILE") {
        Ok(path) if !path.trim().is_empty() => {
            if dotenvy::from_filename(&path).is_err() {
                eprintln!("could not read ENV_FILE={path}");
                std::process::exit(1);
            }
        }
        _ => {
            let _ = dotenvy::dotenv();
        }
    }
}
