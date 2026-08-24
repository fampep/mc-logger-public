//! Finds death messages the classifier does not know yet, adds the safe ones,
//! and backfills the rows they explain.
//!
//! These servers invent new death wording constantly — six new 2b2t templates
//! turned up in a single afternoon — and every one of them silently cost the
//! victim a death and sometimes a player a kill. This closes that loop without
//! anybody watching: it reads the `server` bucket, turns unrecognised lines
//! into templates, writes them to `custom_deaths.json`, and re-runs the
//! backfill. The loggers re-read that file on their own, so nothing restarts.
//!
//!   cargo run --bin discover                 # one pass, reports only
//!   cargo run --bin discover -- --apply      # one pass, writes
//!   cargo run --bin discover -- --apply --watch
//!
//! # What it will and will not add by itself
//!
//! Adding a wrong template is far more expensive than missing a right one. A
//! missed line stays `server` and gets picked up whenever the template is added
//! later; a wrong one writes bad names into stats, and because the backfill
//! refuses to downgrade a death, it sticks.
//!
//! So a candidate is added automatically only when it cannot plausibly be a PvP
//! kill: it must start with a known player, contain exactly one player name,
//! and use vocabulary the existing death templates already use. Anything naming
//! two players is a possible kill whose direction cannot be inferred — "X
//! murked Y" and "X was murdered by Y" put the killer at opposite ends — so
//! those go to a review file with a suggested template instead of being
//! guessed at.

#[allow(dead_code)]
#[path = "../classify.rs"]
mod classify;

// The reporting side of the backfill belongs to `reclassify`; this binary only
// scans and applies.
#[allow(dead_code)]
#[path = "../backfill.rs"]
mod backfill;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use regex::Regex;

/// How long between passes in `--watch` mode. Deaths are not urgent, and each
/// pass rewrites a file the loggers read.
const WATCH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(600);

/// The group new templates are appended to, kept separate from the hand-curated
/// per-server groups so it is obvious what was added without a human looking.
const DISCOVERED_GROUP: &str = "discovered";

/// Lines that name a player but are not deaths.
const NOT_DEATHS: &[&str] = &[
    "joined", "left the", "left.", "welcome", "discord", "vote", "shop", "buy", "rank", "donate", "queue",
    "connecting", "connected", "website", "http", "www.", "afk", "whitelist", "ban", "mute",
    "kick", "promoted", "restarting", "server is", "position in",
    "treasure", "voted", "punished", "reward", "/spin", "gifts",
    "has created", "/cf", "🎁", "airdrop", "✈", "⛃",
    "teleport", "ᴛᴇʟᴇᴘᴏʀᴛ", "tpa", "crates", "rampage",
];

struct Candidate {
    template: String,
    example: String,
    count: i64,
    /// Why it cannot be added automatically, when it cannot.
    blocked: Option<String>,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let apply = args.iter().any(|a| a == "--apply");
    let watch = args.iter().any(|a| a == "--watch");

    let instances = instances()?;
    if instances.is_empty() {
        eprintln!("No .env files with a DATABASE_URL found next to the binary's crate root.");
        std::process::exit(1);
    }

    loop {
        for instance in &instances {
            if let Err(error) = pass(instance, apply).await {
                // One unreachable database must not stop the other, and in
                // watch mode must not end the loop.
                eprintln!("{}: {error}", instance.label);
            }
        }

        if !watch {
            return Ok(());
        }
        tokio::time::sleep(WATCH_INTERVAL).await;
    }
}

struct Instance {
    label: String,
    database_url: String,
}

/// Every logger instance, found the same way the supervisor finds them: one
/// `.env*` file per server, each naming its own database.
fn instances() -> eyre::Result<Vec<Instance>> {
    let root = crate_root();
    let mut instances = Vec::new();

    let Ok(entries) = std::fs::read_dir(&root) else {
        return Ok(instances);
    };

    let mut files: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| {
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else { return false };
            // `.env.disabled` is how an instance is taken out of service.
            name == ".env" || (name.starts_with(".env.") && !name.ends_with(".disabled"))
        })
        .filter(|path| path.extension().and_then(|e| e.to_str()) != Some("pid"))
        .collect();
    files.sort();

    for file in files {
        let Ok(text) = std::fs::read_to_string(&file) else { continue };
        let Some(url) = text.lines().find_map(|line| {
            let line = line.trim();
            line.strip_prefix("DATABASE_URL=").map(|v| v.trim().trim_matches('"').to_owned())
        }) else {
            continue;
        };
        if url.is_empty() {
            continue;
        }
        instances.push(Instance {
            label: file.file_name().unwrap_or_default().to_string_lossy().into_owned(),
            database_url: url,
        });
    }

    Ok(instances)
}

fn crate_root() -> PathBuf {
    std::env::var("AZALEA_BOT_DIR")
        .ok()
        .filter(|dir| !dir.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

async fn pass(instance: &Instance, apply: bool) -> eyre::Result<()> {
    let (mut client, connection) =
        tokio_postgres::connect(&instance.database_url, tokio_postgres::NoTls).await?;
    let handle = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("connection lost: {error}");
        }
    });

    // Every pass re-reads the file, so templates added by hand between passes
    // are respected rather than reported as missing all over again.
    let _ = classify::reload_custom_deaths();

    let players: Vec<String> = client
        .query("SELECT name FROM players", &[])
        .await?
        .iter()
        .map(|row| row.get::<_, String>("name"))
        .collect();

    let rows = client
        .query(
            "SELECT plain_text, count(*) AS n
               FROM chat_messages
              WHERE kind = 'server' AND plain_text <> ''
              GROUP BY plain_text
              ORDER BY n DESC",
            &[],
        )
        .await?;

    let vocabulary = classify::death_vocabulary();
    let mut candidates: HashMap<String, Candidate> = HashMap::new();

    for row in &rows {
        let text: String = row.get("plain_text");
        let count: i64 = row.get("n");
        let text = text.trim().to_owned();

        // Already recognised: the row simply predates the template, and the
        // backfill below will fix it.
        if classify::classify(&text, false, false, None).kind == classify::Kind::Death {
            continue;
        }

        let Some(candidate) = assess(&text, count, &players, &vocabulary) else { continue };

        candidates
            .entry(candidate.template.clone())
            .and_modify(|existing| existing.count += count)
            .or_insert(candidate);
    }

    let (addable, review): (Vec<&Candidate>, Vec<&Candidate>) =
        candidates.values().partition(|c| c.blocked.is_none());

    println!("\n=== {} ===", instance.label);
    if addable.is_empty() && review.is_empty() {
        println!("no unrecognised deaths");
    }

    for candidate in &addable {
        println!("  + {:>3}x  {}", candidate.count, candidate.example);
        println!("         {}", candidate.template);
    }
    for candidate in &review {
        println!("  ? {:>3}x  {}", candidate.count, candidate.example);
        println!(
            "         {}   [{}]",
            candidate.template,
            candidate.blocked.as_deref().unwrap_or("")
        );
    }

    if !apply {
        if !addable.is_empty() || !review.is_empty() {
            println!("\n  (dry run — pass --apply to add and backfill)");
        }
        handle.abort();
        return Ok(());
    }

    let added: Vec<String> = addable.iter().map(|c| c.template.clone()).collect();
    if !added.is_empty() {
        add_templates(&added)?;
        match classify::reload_custom_deaths() {
            Ok(count) => println!("  added {} template(s); {count} patterns now", added.len()),
            Err(error) => eprintln!("  added templates but could not reload: {error}"),
        }
    }

    if !review.is_empty() {
        write_review(&instance.label, &review)?;
        println!(
            "  {} candidate(s) need a decision — see {}",
            review.len(),
            review_path().display()
        );
    }

    // Fix the stored rows the new templates explain. Runs even when nothing was
    // added, so a template added by hand between passes still gets backfilled
    // without anybody remembering to.
    let scan = backfill::scan(&client).await?;
    if !scan.changes.is_empty() {
        let written = backfill::apply(&mut client, &scan.changes).await?;
        println!("  backfilled {written} row(s)");
    }

    handle.abort();
    Ok(())
}

/// Matches a player name only as a whole name, so "Bob" inside "Bob_123" is not
/// mistaken for a second player.
///
/// `\b` rather than lookaround: Rust's `regex` has no lookbehind, and building
/// one silently produced a pattern that never compiled, so every name was
/// skipped and the detector reported a clean database no matter what was in it.
/// `_` counts as a word character, which is exactly the boundary wanted here
/// since Minecraft names are `[A-Za-z0-9_]`.
fn name_pattern(name: &str) -> Option<Regex> {
    match Regex::new(&format!(r"\b{}\b", regex::escape(name))) {
        Ok(pattern) => Some(pattern),
        Err(error) => {
            eprintln!("cannot match player name {name:?}: {error}");
            None
        }
    }
}

/// Turns a line into a template and decides whether it is safe to add.
fn assess(
    text: &str,
    count: i64,
    players: &[String],
    vocabulary: &HashSet<String>,
) -> Option<Candidate> {
    let lower = text.to_lowercase();
    if NOT_DEATHS.iter().any(|word| lower.contains(word)) {
        return None;
    }

    // Names sorted longest first so "Bob123" is found before a shorter "Bob".
    let mut sorted: Vec<&String> = players.iter().collect();
    sorted.sort_by(|a, b| b.len().cmp(&a.len()));

    // Where each distinct player name appears, in order of appearance.
    let mut found: Vec<(usize, &str)> = Vec::new();
    for name in sorted {
        let Some(pattern) = name_pattern(name) else { continue };
        let Some(m) = pattern.find(text) else { continue };
        // Skip a name contained in one already found, so "Bob" inside "Bobby"
        // is not counted as a second player.
        if found.iter().any(|(_, seen)| seen.contains(name.as_str()) || name.contains(seen)) {
            continue;
        }
        found.push((m.start(), name.as_str()));
    }
    found.sort_by_key(|(index, _)| *index);

    // A death names somebody, and every one of these servers puts the subject
    // first. Without that anchor there is nothing to build a template on.
    //
    // "First" allows decoration in front of the name, because two servers put
    // an icon there — "💀 RoninTheBold  drowned", "[☠] Killer 🗡 Victim" — and
    // requiring the name to be the literal first token meant neither server's
    // deaths were ever offered. What must not precede the name is a *word*:
    // that would be a sentence about a player rather than a line starting with
    // one, which is how server notices read.
    let Some(&(index, _)) = found.first() else { return None };
    if text[..index].chars().any(char::is_alphanumeric) {
        return None;
    }

    let mut template = text.to_owned();
    let mut slot = 0usize;
    for (_, name) in found.iter().take(4) {
        slot += 1;
        let Some(pattern) = name_pattern(name) else { continue };
        // NoExpand throughout: `$s` in a replacement is a capture-group
        // reference to the regex crate, so a plain string silently produced
        // "%1" instead of "%1$s" and every generated template was malformed.
        template = pattern
            .replace_all(&template, regex::NoExpand(&format!("%{slot}$s")))
            .into_owned();
    }

    // "by a zombie pigman" -> "by %N$s", stopping at the clause end so trailing
    // wording like "and instantly died." survives. Runs before the weapon rule
    // so the killer takes the lower slot, matching every hand-written template.
    //
    // The clause end is captured and re-emitted rather than matched with a
    // lookahead, which Rust's `regex` does not support — as a lookahead this
    // never compiled, and the rule was skipped in silence.
    if let Ok(mob) = Regex::new(r"\bby (an? [a-z ]+?)(\.| and | while | wielding |$)") {
        if mob.is_match(&template) {
            slot += 1;
            template = mob
                .replace(&template, |caps: &regex::Captures| {
                    format!("by %{slot}$s{}", &caps[2])
                })
                .into_owned();
        }
    }
    if let Ok(weapon) = Regex::new(r"\bwielding\s+(.+?)( and | while |\.$|$)") {
        if weapon.is_match(&template) {
            slot += 1;
            template = weapon
                .replace(&template, |caps: &regex::Captures| {
                    format!("wielding %{slot}$s{}", &caps[2])
                })
                .into_owned();
        }
    }

    if slot == 0 {
        return None;
    }

    // Two players named means this may be a kill, and which one did the killing
    // cannot be read off the grammar reliably. Getting it backwards credits the
    // victim with the kill, so it is never guessed.
    let blocked = if found.len() > 1 {
        Some("names two players — could be a kill in either direction".to_owned())
    } else if !reads_like_a_death(text, vocabulary) {
        Some("no wording any existing death template uses".to_owned())
    } else {
        None
    };

    Some(Candidate { template, example: text.to_owned(), count, blocked })
}

/// Whether the line uses vocabulary the known death templates already use.
fn reads_like_a_death(text: &str, vocabulary: &HashSet<String>) -> bool {
    text.split(|c: char| !c.is_ascii_alphabetic())
        .any(|word| word.len() >= 4 && vocabulary.contains(&word.to_lowercase()))
}

/// Appends templates to the `discovered` group, preserving everything else.
///
/// Written to a temporary file and renamed, because the loggers read this file
/// on a timer and must never see it half-written.
fn add_templates(templates: &[String]) -> eyre::Result<()> {
    let path = classify::custom_deaths_path();
    let raw = std::fs::read_to_string(&path)?;
    let mut root: serde_json::Value = serde_json::from_str(&raw)?;

    let Some(object) = root.as_object_mut() else {
        eyre::bail!("{} is not a JSON object", path.display());
    };

    let group = object
        .entry(DISCOVERED_GROUP)
        .or_insert_with(|| serde_json::Value::Array(Vec::new()));
    let Some(list) = group.as_array_mut() else {
        eyre::bail!("`{DISCOVERED_GROUP}` is not a list");
    };

    for template in templates {
        list.push(serde_json::Value::String(template.clone()));
    }

    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, serde_json::to_string_pretty(&root)?)?;
    std::fs::rename(&temporary, &path)?;
    Ok(())
}

fn review_path() -> PathBuf {
    classify::custom_deaths_path().with_file_name("discovered_review.json")
}

/// Candidates a human has to rule on, written where they can be pasted straight
/// into `custom_deaths.json` once the direction is confirmed.
fn write_review(label: &str, candidates: &[&Candidate]) -> eyre::Result<()> {
    let path = review_path();

    let mut existing: serde_json::Map<String, serde_json::Value> =
        std::fs::read_to_string(&path)
            .ok()
            .and_then(|raw| serde_json::from_str(&raw).ok())
            .unwrap_or_default();

    existing.insert(
        "_readme".to_owned(),
        serde_json::json!([
            "Death messages discover found but would not add by itself.",
            "Check the direction, then move the template into custom_deaths.json.",
            "A line naming two players is usually a kill: if the killer comes",
            "first, use {\"template\": ..., \"killer\": 1, \"victim\": 2}.",
            "Entries here are rewritten every pass; deleting one is harmless."
        ]),
    );

    let entries: Vec<serde_json::Value> = candidates
        .iter()
        .map(|candidate| {
            serde_json::json!({
                "template": candidate.template,
                "example": candidate.example,
                "seen": candidate.count,
                "why": candidate.blocked,
            })
        })
        .collect();
    existing.insert(label.to_owned(), serde_json::Value::Array(entries));

    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, serde_json::to_string_pretty(&existing)?)?;
    std::fs::rename(&temporary, &path)?;
    Ok(())
}
