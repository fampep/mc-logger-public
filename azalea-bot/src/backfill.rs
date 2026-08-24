//! Re-running the classifier over rows already stored.
//!
//! Shared by `reclassify` (run by hand, prints what it did) and `discover`
//! (runs on a timer, applies quietly after adding a template). Both must agree
//! exactly on what a stored row should become, so the logic lives here once.

use crate::classify::{self, Kind};

// NOT USABLE AGAINST r8+ WITHOUT A PORT, and discover.service is disabled until
// it gets one. The write paths below still target the pre-r8 shape:
//
//   * they UPDATE `chat_messages`, which has been a view since r8 rather than a
//     table -- the physical table is `chat_messages_raw`;
//   * they write `sender_prefix`, `sender_name` and `content`, all of which r8
//     dropped. Names now live in `name_dict` and are referenced by INT4, so a
//     rewrite has to intern a name and store its id, not a string;
//   * `discover.rs` also calls `classify::death_vocabulary()`, which no longer
//     exists and has no direct replacement -- see `death_matcher_count()` and
//     `reload_custom_deaths()` for what the matcher exposes now.
//
// The read path and the classification logic here are still correct, which is
// why the SELECTs have been brought up to date rather than deleted.

/// A row whose classification changed, and what it should become.
pub struct Change {
    pub id: i64,
    pub was_kind: String,
    pub was_subject: Option<String>,
    pub was_killer: Option<String>,
    /// What the row should be now: `death` or `chat`.
    pub kind: String,
    pub subject: Option<String>,
    pub killer: Option<String>,
    pub sender: Option<String>,
    pub prefix: Option<String>,
    pub content: Option<String>,
    pub plain_text: String,
}

impl Change {
    /// A line that was not previously a death at all, as opposed to one that
    /// was a death with the wrong names on it.
    pub fn is_rescue(&self) -> bool {
        self.was_kind != "death"
    }
}

#[derive(Default)]
pub struct Scan {
    pub scanned: usize,
    pub changes: Vec<Change>,
    pub unchanged: usize,
    pub not_a_death: usize,
    /// Rows stored as deaths that no longer match anything. Never written back.
    pub downgrades: Vec<String>,
}

/// Classifies every stored row again and reports what would change.
///
/// Only the two buckets a death can be hiding in are considered. Chat, joins
/// and leaves are left alone: reclassifying those would mean inventing matching
/// `player_events` rows, which is a different job from fixing stats.
pub async fn scan(client: &tokio_postgres::Client) -> eyre::Result<Scan> {
    let rows = client
        .query(
            // translate_key was dropped in r8: servers that send real
            // translation keys are handled at capture time now, and stored
            // rows only ever have the flattened text to go on.
            "SELECT id, source, kind, subject_name, killer_name, plain_text
               FROM chat_messages
              WHERE kind IN ('server', 'death')
              ORDER BY id",
            &[],
        )
        .await?;

    let mut scan = Scan { scanned: rows.len(), ..Default::default() };

    for row in &rows {
        let id: i64 = row.get("id");
        let source: String = row.get("source");
        let kind: String = row.get("kind");
        let was_subject: Option<String> = row.get("subject_name");
        let was_killer: Option<String> = row.get("killer_name");
        let plain_text: String = row.get("plain_text");

        // Reproduces exactly what the logger saw: whether the line arrived on a
        // player packet is recoverable from `source`, and whispers are stored
        // under their own kind so none are in this set.
        let result = classify::classify(
            &plain_text,
            source == "player",
            false,
            None,
        );

        // A `server` row that now reads as chat is rescued too. That happens
        // whenever a server's chat format is taught to the classifier after the
        // fact — Rempolon's "RANK Name ➤ message" filed every message as
        // `server` until the pattern existed. Chat is safe to rewrite where
        // join and leave are not: a chat row has no matching `player_events`
        // row to invent.
        let rescue_as_chat = kind == "server" && result.kind == Kind::Chat && result.sender.is_some();

        if result.kind != Kind::Death && !rescue_as_chat {
            if kind == "death" {
                // Would mean a template stopped matching something it used to.
                // Losing a death is worse than keeping a stale one, so this is
                // only ever reported.
                scan.downgrades.push(plain_text);
            } else {
                scan.not_a_death += 1;
            }
            continue;
        }

        if kind == "death" && was_subject == result.subject && was_killer == result.killer {
            scan.unchanged += 1;
            continue;
        }

        scan.changes.push(Change {
            id,
            was_kind: kind,
            was_subject,
            was_killer,
            kind: result.kind.as_str().to_owned(),
            subject: result.subject,
            killer: result.killer,
            sender: result.sender,
            prefix: result.sender_label,
            content: result.content,
            plain_text,
        });
    }

    Ok(scan)
}

/// Writes the corrections in one transaction.
///
/// A half-applied backfill would leave stats wrong in a way that is harder to
/// reason about than not having run it at all.
pub async fn apply(
    client: &mut tokio_postgres::Client,
    changes: &[Change],
) -> eyre::Result<usize> {
    if changes.is_empty() {
        return Ok(0);
    }

    let transaction = client.transaction().await?;
    // Every column the classifier owns is written, so the row ends up exactly
    // as the logger would have stored it had the pattern existed at the time.
    let statement = transaction
        .prepare(
            "UPDATE chat_messages
                SET kind = $2, subject_name = $3, killer_name = $4,
                    sender_name = $5, content = $6, sender_prefix = $7
              WHERE id = $1",
        )
        .await?;

    for change in changes {
        transaction
            .execute(
                &statement,
                &[
                    &change.id,
                    &change.kind,
                    &change.subject,
                    &change.killer,
                    &change.sender,
                    &change.content,
                    &change.prefix,
                ],
            )
            .await?;
    }
    transaction.commit().await?;

    Ok(changes.len())
}

/// Fills in `sender_prefix` on chat rows stored before the column existed.
///
/// The main scan only looks at the `server` and `death` buckets, so rows that
/// were already correctly filed as chat never get revisited — and every one of
/// them lost its rank, which is the only place that rank was ever recorded.
/// Only the prefix is written: the kind, sender and content of these rows were
/// right when they were stored and must not be second-guessed here.
///
/// Player packets are skipped. Their sender arrives as a packet field with no
/// decoration to recover, so there is nothing to fill in.
pub async fn backfill_prefixes(client: &mut tokio_postgres::Client) -> eyre::Result<usize> {
    let rows = client
        .query(
            "SELECT id, plain_text FROM chat_messages
              WHERE kind = 'chat' AND source <> 'player' AND sender_prefix IS NULL",
            &[],
        )
        .await?;

    let mut updates: Vec<(i64, String)> = Vec::new();
    for row in &rows {
        let id: i64 = row.get("id");
        let plain_text: String = row.get("plain_text");
        if let Some(prefix) = classify::classify(&plain_text, false, false, None).sender_label {
            updates.push((id, prefix));
        }
    }

    if updates.is_empty() {
        return Ok(0);
    }

    let transaction = client.transaction().await?;
    let statement = transaction
        .prepare("UPDATE chat_messages SET sender_prefix = $2 WHERE id = $1")
        .await?;
    for (id, prefix) in &updates {
        transaction.execute(&statement, &[id, prefix]).await?;
    }
    transaction.commit().await?;

    Ok(updates.len())
}
