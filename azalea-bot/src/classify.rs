//! Works out what a chat line actually is.
//!
//! Translation keys are authoritative when the server sends them (vanilla), and
//! text patterns are the fallback for servers that flatten everything to plain
//! text before sending it — which is most of them, and the reason the death
//! matchers below exist at all.

use std::sync::LazyLock;

use regex::Regex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Chat,
    Whisper,
    Join,
    Leave,
    Death,
    Advancement,
    Server,
    Unknown,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        // Single-letter codes in Postgres — Discord expands for display/config.
        match self {
            Kind::Chat => "c",
            Kind::Whisper => "w",
            Kind::Join => "j",
            Kind::Leave => "l",
            Kind::Death => "d",
            Kind::Advancement => "a",
            Kind::Server => "s",
            Kind::Unknown => "u",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Classification {
    pub kind: Kind,
    /// For deaths and join/leave this is the player the line is *about*, which
    /// is not the same as the player who sent it.
    pub subject: Option<String>,
    /// Whoever did the killing, for deaths phrased "... by X". May be a mob
    /// ("Zombie") rather than a player — resolving which is a query concern,
    /// since only the players table knows who is real.
    pub killer: Option<String>,
    /// Set when a *system* line was recognised as player chat. Most servers
    /// reformat chat through plugins and send it as a system message, so the
    /// sender has to be recovered from the text.
    /// Bare Minecraft name — used for heads, stats, and player lookups.
    pub sender: Option<String>,
    pub content: Option<String>,
    /// Full decorated speaker as shown in-game (`[8] [MVP] DesBob`, `<[M] SawgerGG>`).
    pub sender_label: Option<String>,
}

impl Default for Kind {
    fn default() -> Self {
        Kind::Unknown
    }
}

impl Classification {
    fn of(kind: Kind) -> Self {
        Self { kind, ..Default::default() }
    }
}

/// Minecraft names are 1-16 of [A-Za-z0-9_]; some proxies prefix Bedrock players.
const NAME: &str = r"[*.]?[A-Za-z0-9_]{1,16}";

static JOIN: LazyLock<Regex> = LazyLock::new(|| {
    // Case-insensitive verb: UneasyVanilla announces `Sfxm Joined.`,
    // vanilla says `Sfxm joined the game`.
    Regex::new(&format!(r"^({NAME}) (?i:joined the game|joined the server|joined)\.?$"))
        .unwrap()
});

/// Minewind announces arrivals as `Welcome Name!`. `Welcome to Constantiam!`
/// has more words after the first token, so it does not match.
static WELCOME: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(r"^Welcome ({NAME})!$")).unwrap()
});

static LEAVE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"^({NAME}) (?i:left the game|left the server|left|disconnected|quit)\.?$"
    ))
    .unwrap()
});

/// Vanilla `<Name> message`, rank tags before or inside the brackets:
///
///   <wzolt> hello
///   [ʙᴏᴏsᴛᴇʀ] <w1shlol> come get keywalks     (VanillaPlus)
///   <[M] SawgerGG> » want to dupe?            (HaZeyNetwork)
///   [m1rka114] <[M] m1rka114> » hello         (HaZey whisper)
///
/// Notices like `[Vanilla+] Crystal Zone will reset` have no `<Name>` after
/// the tag and do not match.
static ANGLE_CHAT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"^((?:\[[^\]]+\]\s*){{0,3}})<(?:\[[^\]]+\]\s*)?({NAME})>\s?(?:[»>]\s?)?([\s\S]*)$"
    ))
    .unwrap()
});

/// Plugin formats like `Name > message` or `Name » message`, allowing rank
/// decoration between the name and the separator ("Raekuuro ⛏ > nice").
///
/// Only symbol decoration is permitted, not arbitrary words — `.*` here would
/// swallow server notices that happen to contain a `>`. Runs last regardless,
/// after join/leave/death have had their chance.
static PREFIXED_CHAT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"^(?:[^\w\s]+\s*)*({NAME})(?:\s+[^\w\s]+)*\s*[»>]\s?(.+)$"
    ))
    .unwrap()
});

/// `Name: message` and `Rank.Name: message` (Minewind).
static COLON_CHAT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"^((?:[^\w\s]+\s*)*(?:[A-Za-z0-9_]{{1,16}}\.)?)({NAME}):\s(.+)$"
    ))
    .unwrap()
});

/// Bracket rank/level prefixes, and cosmetic symbols, before the name
/// (6b6t, HaZey, many SMPs):
///
///   [Prime] TheGroupProject » Ranked Players Only!
///   [8] [MVP] DesBob [tag] » ty though
///   jointedx21 » hello
///   ❄ santiahre » gg
static TAGGED_CHAT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"^(?:[^\w\s]+\s*)*(?:\[[^\]]+\]\s*)*({NAME})(?:\s+\[[^\]]+\])*\s*[»>]\s?(.+)$"
    ))
    .unwrap()
});

/// Rempolon sends player chat as system messages with rank words and ➤:
///
///   ᴘʟᴀʏᴇʀ+ Cloudcon ➤ man..
///   ULTRA GrimVoidX ➤ yoo chill mate
///   [mitropolzka] ᴘʟᴀʏᴇʀ mitropolzka ➤ @GrimVoidX language
static REMPOLON_CHAT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"^(?:\[[^\]]+\]\s*)?(?:\S+\+?\s+)?({NAME})\s*➤\s?([\s\S]*)$"
    ))
    .unwrap()
});

static TABLIST_JOIN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(r"^\[\+\]\s*({NAME})$")).unwrap()
});

static TABLIST_LEAVE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(r"^\[✖\]\s*({NAME})$")).unwrap()
});

/// Plugin "speakers" that use `Name: message` the same way players do.
///
/// Minewind answers `-explain` and `-luck` this way. CuteSMP sends quest
/// reminders as a "Quests" speaker. 6b6t brands the server itself as a chat
/// speaker (`6b6t » motd`). None of them are players, so they must not get a
/// sender or a player head in the feed.
///
/// `6Builders6Tools` is deliberately *not* in this list. It posts with a rank
/// prefix like any other player (`[APE 🍰] 6Builders6Tools » …`), and its lines
/// are wanted as chat.
pub fn is_plugin_speaker(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "explainer"
            | "debugmenu"
            | "quests"
            | "6b6t"
            | "sixbsixt"
    )
}

/// CuteSMP: `[name] Rank name [tag]: message` (bracket name must equal speaker).
static CUTESMP_CHAT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"^\[({NAME})\]\s+(?:\S+\s+)?({NAME})(?:\s+\[[^\]]+\])?:\s*(.+)$"
    ))
    .unwrap()
});

/// `.lenalovesyogurt [i] come get your free ticket!`
static NAME_TAG_CHAT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(r"^({NAME})\s+\[[^\]]{{1,24}}\]\s+(.+)$")).unwrap()
});

fn is_legacy_code_char(c: char) -> bool {
    matches!(
        c,
        '0'..='9' | 'a'..='f' | 'k'..='o' | 'r' | 'x' | 'A'..='F' | 'K'..='O' | 'R' | 'X'
    )
}

/// Strip Minecraft `§X` colour/format codes. Leaves `&` alone so chat like
/// "Tom & Jerry" is not eaten.
pub fn strip_section_sign(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '§' {
            let _ = chars.next();
            continue;
        }
        out.push(c);
    }
    out
}

/// Strip `§X` and plugin `&X` codes from a name.
pub fn strip_legacy_formatting(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '§' {
            let _ = chars.next();
            continue;
        }
        if c == '&' {
            match chars.next() {
                Some(code) if is_legacy_code_char(code) => continue,
                Some(other) => {
                    out.push(c);
                    out.push(other);
                }
                None => {}
            }
            continue;
        }
        out.push(c);
    }
    out
}

fn is_only_legacy_codes(s: &str) -> bool {
    !s.is_empty() && s.chars().all(is_legacy_code_char)
}

/// True when `raw` is a plausible Minecraft name after stripping colour codes.
/// CuteSMP tab-list NPCs are only `§` codes; after strip they look like `1rof08cr`.
pub fn clean_player_name(raw: &str) -> Option<String> {
    let had_fmt = raw.contains('§') || raw.contains('&');
    clean_named(raw, had_fmt)
}

fn clean_named(raw: &str, had_fmt: bool) -> Option<String> {
    let stripped = strip_legacy_formatting(raw);
    let name = stripped.trim();
    if name.is_empty() {
        return None;
    }
    let body = name.strip_prefix('.').unwrap_or(name);
    if body.is_empty() || body.len() > 16 || name.len() > 17 {
        return None;
    }
    if !body.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return None;
    }
    // Leftover colour-code alphabet after stripping a dummy (`§1§r§o§f…` → `1rof08cr`).
    // Only when the original had formatting, so real names like `bob` / `cool` stay.
    if had_fmt && is_only_legacy_codes(body) {
        return None;
    }
    Some(name.to_owned())
}

/// System junk that must not hit Discord even when `BRIDGE_KINDS` includes server.
pub fn is_bridge_spam(text: &str) -> bool {
    let plain = strip_section_sign(text);
    let collapsed = plain.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        return false;
    }
    let lower = collapsed.to_ascii_lowercase();
    if lower == "unknown command." || lower == "unknown command" {
        return true;
    }
    if lower.contains("remember to /vote") || lower.contains("/vote to get free rewards") {
        return true;
    }
    if lower.contains("daily quests to complete") {
        return true;
    }
    if lower.starts_with("quests") && lower.contains("quest") {
        return true;
    }
    if lower.contains("you're protected from attack") || lower.contains("you are protected from attack")
    {
        return true;
    }
    if lower.contains("[ welcome ]") {
        return true;
    }
    false
}

static ADVANCEMENT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"^({NAME}) has (?:made the advancement|completed the challenge|reached the goal)\b"
    ))
    .unwrap()
});

/// A template plus which capture slots hold the victim and the killer.
///
/// Most servers phrase deaths victim-first, so a plain string means victim in
/// slot 1. Some (especially 2b2t) phrase them killer-first ("SaltyNew
/// assassinated JamesIsAlive with X"), which is an object naming the slots.
struct CustomTemplate {
    template: String,
    victim_group: usize,
    killer_group: Option<usize>,
}

/// Built-in copy of `custom_deaths.json` (2b2t, 6b6t, 9b9t, Constantiam, …).
const BUILT_IN_CUSTOM_DEATHS: &str = include_str!("custom_deaths.json");

/// Editable template file. Override with `CUSTOM_DEATHS_PATH`, otherwise
/// `src/custom_deaths.json` relative to the working directory.
pub fn custom_deaths_path() -> std::path::PathBuf {
    std::env::var("CUSTOM_DEATHS_PATH")
        .ok()
        .filter(|path| !path.trim().is_empty())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("src/custom_deaths.json"))
}

fn custom_deaths_source() -> String {
    std::fs::read_to_string(custom_deaths_path()).unwrap_or_else(|_| BUILT_IN_CUSTOM_DEATHS.to_owned())
}

fn custom_death_templates(raw: &str) -> Vec<CustomTemplate> {
    let groups: std::collections::HashMap<String, serde_json::Value> = match serde_json::from_str(raw)
    {
        Ok(groups) => groups,
        Err(error) => {
            eprintln!("custom_deaths.json is not valid JSON: {error}");
            return Vec::new();
        }
    };

    let mut templates = Vec::new();
    for (key, value) in groups {
        if key.starts_with('_') {
            continue;
        }
        let Some(list) = value.as_array() else { continue };

        for entry in list {
            if let Some(text) = entry.as_str() {
                templates.push(CustomTemplate {
                    template: text.to_owned(),
                    victim_group: 1,
                    killer_group: if text.contains("by %2$s") { Some(2) } else { None },
                });
            } else if let Some(object) = entry.as_object() {
                let Some(text) = object.get("template").and_then(|v| v.as_str()) else {
                    continue;
                };
                templates.push(CustomTemplate {
                    template: text.to_owned(),
                    victim_group: object.get("victim").and_then(|v| v.as_u64()).unwrap_or(1) as usize,
                    killer_group: object.get("killer").and_then(|v| v.as_u64()).map(|v| v as usize),
                });
            }
        }
    }
    templates
}

/// Every `death.*` entry turned into a matcher, so deaths are still recognised
/// when the server sends them as flat text rather than a translatable
/// component. Built from the vendored Minecraft en_us table plus
/// `custom_deaths.json` (2b2t / 6b6t / …).
static DEATH_MATCHERS: LazyLock<std::sync::RwLock<std::sync::Arc<Vec<DeathMatcher>>>> =
    LazyLock::new(|| {
        std::sync::RwLock::new(std::sync::Arc::new(build_death_matchers(&custom_deaths_source())))
    });

/// Re-reads the template file and swaps the matchers in.
#[allow(dead_code)] // available for live reload / ops tooling
pub fn reload_custom_deaths() -> Result<usize, String> {
    let path = custom_deaths_path();
    let raw = std::fs::read_to_string(&path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;

    serde_json::from_str::<serde_json::Value>(&raw)
        .map_err(|error| format!("{} is not valid JSON: {error}", path.display()))?;

    let matchers = build_death_matchers(&raw);
    if matchers.is_empty() {
        return Err(format!("{} produced no matchers", path.display()));
    }

    let count = matchers.len();
    match DEATH_MATCHERS.write() {
        Ok(mut current) => *current = std::sync::Arc::new(matchers),
        Err(error) => return Err(format!("matcher lock poisoned: {error}")),
    }
    Ok(count)
}

struct DeathMatcher {
    regex: Regex,
    victim_group: usize,
    /// Killer capture when the template names one. Plain "... by %2$s" lines and
    /// explicit killer-first objects set this; "while fighting %2$s" bystanders do not.
    killer_group: Option<usize>,
}

fn build_death_matchers(custom_source: &str) -> Vec<DeathMatcher> {
    let raw = include_str!("death_messages.json");
    let table: std::collections::HashMap<String, String> = match serde_json::from_str(raw) {
        Ok(table) => table,
        Err(_) => return Vec::new(),
    };

    let custom = custom_death_templates(custom_source);

    let mut templates: Vec<(&str, usize, Option<usize>)> = custom
        .iter()
        .map(|c| (c.template.as_str(), c.victim_group, c.killer_group))
        .chain(table.values().map(|t| {
            let killer = if t.contains("by %2$s") { Some(2) } else { None };
            (t.as_str(), 1usize, killer)
        }))
        .collect();

    // Longest / most specific first. HashMap iteration is nondeterministic, and
    // loose templates would otherwise steal kills from precise ones.
    templates.sort_by(|a, b| b.0.len().cmp(&a.0.len()).then_with(|| a.0.cmp(b.0)));

    let mut matchers = Vec::new();
    for (template, victim_group, killer_group) in templates {
        let mut pattern = regex::escape(template);
        for slot in 1..=4 {
            let escaped = regex::escape(&format!("%{slot}$s"));
            // Victim is a player name. Killer may be a mob with spaces. Other
            // slots (weapons) may be empty — 2b2t sometimes renders blank items.
            let group = if slot == victim_group {
                format!("({NAME})")
            } else if Some(slot) == killer_group {
                "(.+?)".to_owned()
            } else {
                "(.*?)".to_owned()
            };
            pattern = pattern.replace(&escaped, &group);
        }

        if let Ok(regex) = Regex::new(&format!("^{pattern}$")) {
            matchers.push(DeathMatcher { regex, victim_group, killer_group });
        }
    }

    matchers
}

/// How many death patterns were built. Logged at startup so a broken table is
/// obvious rather than silently classifying every death as "server".
pub fn death_matcher_count() -> usize {
    DEATH_MATCHERS.read().map(|matchers| matchers.len()).unwrap_or(0)
}

/// Every word the known death templates use, lowercased.
///
/// `discover` decides whether an unrecognised server line *reads* like a death
/// before proposing it as a template, and this is what that is measured
/// against. Built from the same two sources as the matchers themselves — the
/// vanilla table and `custom_deaths.json` — so adding a template widens the
/// vocabulary with it rather than leaving the two to drift apart.
///
/// Words shorter than four letters are dropped: "the", "was" and "by" appear in
/// nearly every line on a Minecraft server and would match anything.
#[allow(dead_code)] // used by the `discover` binary, not by the logger
pub fn death_vocabulary() -> std::collections::HashSet<String> {
    let mut words = std::collections::HashSet::new();
    let mut add = |template: &str| {
        // Splitting on non-alphabetic also disposes of the `%1$s` slots: the
        // lone `s` they leave behind is one character and falls under the limit.
        for word in template.split(|c: char| !c.is_ascii_alphabetic()) {
            if word.len() >= 4 {
                words.insert(word.to_lowercase());
            }
        }
    };

    if let Ok(table) = serde_json::from_str::<std::collections::HashMap<String, String>>(
        include_str!("death_messages.json"),
    ) {
        for template in table.values() {
            add(template);
        }
    }
    for custom in custom_death_templates(&custom_deaths_source()) {
        add(&custom.template);
    }
    words
}

/// Returns (victim, killer) for the first death template that matches.
fn match_death(text: &str) -> Option<(String, Option<String>)> {
    let Ok(guard) = DEATH_MATCHERS.read() else {
        return None;
    };
    let matchers = std::sync::Arc::clone(&guard);
    drop(guard);

    for matcher in matchers.iter() {
        let Some(captures) = matcher.regex.captures(text) else {
            continue;
        };

        let victim = captures
            .get(matcher.victim_group)
            .map(|m| tidy_captured_name(m.as_str()))
            .unwrap_or_default();
        let killer = matcher
            .killer_group
            .and_then(|group| captures.get(group))
            .map(|m| tidy_captured_name(m.as_str()))
            .filter(|name| !name.is_empty());

        return Some((victim, killer));
    }
    None
}

/// Strip junk that rides along in a capture so the name still joins `players`.
///
/// VanillaPlus appends the killer's ping in parentheses ("Fractuerd (410)").
/// 2b2t leaves a trailing full stop inside vanilla captures. Neither belongs
/// on a name that has to match the `players` table.
fn tidy_captured_name(raw: &str) -> String {
    let mut name = raw.trim().trim_end_matches(['.', '!']).trim_end().to_owned();
    if let Some(stripped) = name.strip_suffix(')') {
        if let Some((head, ping)) = stripped.rsplit_once(" (") {
            if !head.is_empty() && !ping.is_empty() && ping.chars().all(|c| c.is_ascii_digit()) {
                name = head.trim_end().to_owned();
            }
        }
    }
    name
}

/// Speaker decoration before the message text (ranks, level, angle brackets).
/// Seconds left on a server restart countdown, if this line is one.
///
/// Every server words it differently. These are the real forms, taken from the
/// logs of the three servers this runs against:
///
///   6b6t         `Server restarts in 1m 15s`, `Server restarts in 1s`
///                `This proxy will restart in 10m.`, `This proxy is restarting now.`
///   2b2t         `Server restarting in 1 second...`, `... in 2 minutes...`
///   vanillaplus  `RESTART | Server restarting in 1m 00s`
///                `The server will restart in 30s.`
///
/// Only ever called on system lines, so player chat about restarts cannot
/// trigger it.
pub fn restart_countdown_secs(text: &str) -> Option<u64> {
    let lower = strip_section_sign(text).to_lowercase();
    if !lower.contains("restart") {
        return None;
    }
    // "is restarting now" has no number but is the most urgent form there is.
    if lower.contains("restarting now") || lower.contains("restart now") {
        return Some(0);
    }

    static DURATION: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(
            r"(?:\bin\b|:)\s*(?:(\d+)\s*(?:m\b|min(?:ute)?s?))?\s*(?:(\d+)\s*(?:s\b|sec(?:ond)?s?))?",
        )
        .unwrap()
    });

    for caps in DURATION.captures_iter(&lower) {
        let mins = caps.get(1).and_then(|m| m.as_str().parse::<u64>().ok());
        let secs = caps.get(2).and_then(|m| m.as_str().parse::<u64>().ok());
        if mins.is_none() && secs.is_none() {
            continue;
        }
        return Some(mins.unwrap_or(0) * 60 + secs.unwrap_or(0));
    }
    None
}

pub fn label_before_content(text: &str, content: &str) -> Option<String> {
    let content = content.trim();
    if content.is_empty() {
        return None;
    }
    let trimmed = text.trim();
    for separator in ['»', '➤'] {
        if let Some((head, tail)) = trimmed.split_once(separator) {
            if tail.trim() == content {
                let label = head.trim();
                if !label.is_empty() {
                    return Some(label.to_owned());
                }
            }
        }
    }
    if trimmed.ends_with(content) {
        let label = trimmed[..trimmed.len() - content.len()].trim_end();
        if !label.is_empty() {
            return Some(label.to_owned());
        }
    }
    None
}

fn chat_result(text: &str, sender: String, content: String) -> Classification {
    Classification {
        kind: Kind::Chat,
        sender: Some(sender),
        content: Some(content.clone()),
        sender_label: label_before_content(text, &content),
        ..Default::default()
    }
}

fn named_from_line(had_fmt: bool, captured: &str) -> Option<String> {
    clean_named(
        captured,
        had_fmt || captured.contains('§') || captured.contains('&'),
    )
}

fn nameless_presence(plain: &str) -> Option<Kind> {
    let t = plain.trim().trim_end_matches('.').trim().to_ascii_lowercase();
    match t.as_str() {
        "joined the game" | "joined the server" | "joined" => Some(Kind::Join),
        "left the game" | "left the server" | "left" | "disconnected" | "quit" => Some(Kind::Leave),
        _ => None,
    }
}

fn chat_from_capture(plain: &str, sender: String, content: String) -> Classification {
    if is_plugin_speaker(&sender) || is_bridge_spam(plain) || is_bridge_spam(&content) {
        return if is_bridge_spam(plain) || is_bridge_spam(&content) {
            Classification::of(Kind::Unknown)
        } else {
            Classification::of(Kind::Server)
        };
    }
    let Some(sender) = named_from_line(plain.contains('§'), &sender) else {
        return Classification::of(Kind::Server);
    };
    chat_result(plain, sender, content)
}

/// `is_player_packet` distinguishes a real player-chat packet from a system
/// line, and `translate_key` is used ahead of any text matching when present.
pub fn classify(
    text: &str,
    is_player_packet: bool,
    is_whisper: bool,
    translate_key: Option<&str>,
) -> Classification {
    if is_whisper {
        return Classification::of(Kind::Whisper);
    }

    // Vanilla servers say exactly what a message is; believe them.
    if let Some(key) = translate_key {
        if key.starts_with("death.") {
            return Classification::of(Kind::Death);
        }
        if key.starts_with("chat.type.advancement") {
            return Classification::of(Kind::Advancement);
        }
        match key {
            "multiplayer.player.joined" => return Classification::of(Kind::Join),
            "multiplayer.player.left" => return Classification::of(Kind::Leave),
            "chat.type.text" => return Classification::of(Kind::Chat),
            _ => {}
        }
    }

    let had_fmt = text.contains('§') || text.contains('&');
    let plain = strip_section_sign(text);
    let plain = plain.trim();

    // A player-chat packet is chat by definition, whatever the text looks like.
    // Plugin speakers (CuteSMP "Quests") still have to be filtered in handle_chat
    // because the sender lives on the packet, not in this text.
    if is_player_packet {
        return Classification::of(Kind::Chat);
    }

    if is_bridge_spam(plain) {
        return Classification::of(Kind::Unknown);
    }

    // `<Name> message` is unambiguous, so it wins over every text pattern below.
    // Constantiam reformats chat into system messages in exactly this shape,
    // which otherwise fell through and got recorded as "server".
    if let Some(captures) = ANGLE_CHAT.captures(plain) {
        let sender = captures.get(2).map(|m| m.as_str().to_owned()).unwrap_or_default();
        let content = captures.get(3).map(|m| m.as_str().to_owned()).unwrap_or_default();
        return chat_from_capture(plain, sender, content);
    }

    if let Some(captures) = JOIN.captures(plain)
        .or_else(|| WELCOME.captures(plain))
        .or_else(|| TABLIST_JOIN.captures(plain))
    {
        return Classification {
            kind: Kind::Join,
            subject: captures.get(1).and_then(|m| named_from_line(had_fmt, m.as_str())),
            ..Default::default()
        };
    }

    if let Some(captures) = LEAVE.captures(plain).or_else(|| TABLIST_LEAVE.captures(plain)) {
        return Classification {
            kind: Kind::Leave,
            subject: captures.get(1).and_then(|m| named_from_line(had_fmt, m.as_str())),
            ..Default::default()
        };
    }

    // Dummy tab-list NPCs are only colour codes. After stripping `§X` the line
    // is just "left the game" / "joined the game" with no name.
    if let Some(kind) = nameless_presence(plain) {
        return Classification { kind, subject: None, ..Default::default() };
    }

    if let Some(captures) = ADVANCEMENT.captures(plain) {
        return Classification {
            kind: Kind::Advancement,
            subject: captures.get(1).and_then(|m| named_from_line(had_fmt, m.as_str())),
            ..Default::default()
        };
    }

    // Runs after join/leave so "X left the game" is not read as a death.
    if let Some((victim, killer)) = match_death(plain) {
        let victim = named_from_line(had_fmt, &victim);
        if victim.is_none() {
            return Classification::of(Kind::Unknown);
        }
        return Classification {
            kind: Kind::Death,
            subject: victim,
            killer,
            ..Default::default()
        };
    }

    if let Some(captures) = CUTESMP_CHAT.captures(plain) {
        let bracket = captures.get(1).map(|m| m.as_str()).unwrap_or("");
        let speaker = captures.get(2).map(|m| m.as_str()).unwrap_or("");
        if bracket.eq_ignore_ascii_case(speaker) {
            let content = captures.get(3).map(|m| m.as_str().to_owned()).unwrap_or_default();
            if is_plugin_speaker(speaker) || is_bridge_spam(plain) || is_bridge_spam(&content) {
                return if is_bridge_spam(plain) || is_bridge_spam(&content) {
                    Classification::of(Kind::Unknown)
                } else {
                    Classification::of(Kind::Server)
                };
            }
            if let Some(sender) = named_from_line(had_fmt, speaker) {
                return Classification {
                    kind: Kind::Chat,
                    sender: Some(sender),
                    content: Some(content),
                    sender_label: None,
                    ..Default::default()
                };
            }
        }
    }

    if let Some(captures) = TAGGED_CHAT.captures(plain) {
        let sender = captures.get(1).map(|m| m.as_str().to_owned()).unwrap_or_default();
        let content = captures.get(2).map(|m| m.as_str().to_owned()).unwrap_or_default();
        return chat_from_capture(plain, sender, content);
    }

    if let Some(captures) = REMPOLON_CHAT.captures(plain) {
        let sender = captures.get(1).map(|m| m.as_str().to_owned()).unwrap_or_default();
        let content = captures.get(2).map(|m| m.as_str().to_owned()).unwrap_or_default();
        return chat_from_capture(plain, sender, content);
    }

    // Name-first with emoji decoration between name and separator ("Raekuuro ⛏ > nice").
    if let Some(captures) = PREFIXED_CHAT.captures(plain) {
        let sender = captures.get(1).map(|m| m.as_str().to_owned()).unwrap_or_default();
        let content = captures.get(2).map(|m| m.as_str().to_owned()).unwrap_or_default();
        return chat_from_capture(plain, sender, content);
    }

    if let Some(captures) = COLON_CHAT.captures(plain) {
        let sender = captures.get(2).map(|m| m.as_str().to_owned());
        if sender.as_deref().is_some_and(is_plugin_speaker) {
            return if is_bridge_spam(plain) {
                Classification::of(Kind::Unknown)
            } else {
                Classification::of(Kind::Server)
            };
        }
        let content = captures.get(3).map(|m| m.as_str().to_owned()).unwrap_or_default();
        if is_bridge_spam(plain) || is_bridge_spam(&content) {
            return Classification::of(Kind::Unknown);
        }
        let rank = captures.get(1).map(|m| m.as_str().trim()).filter(|s| !s.is_empty());
        let Some(sender) = sender.as_deref().and_then(|s| named_from_line(had_fmt, s)) else {
            return Classification::of(Kind::Server);
        };
        let label = rank.map(|r| format!("{r}{sender}")).or_else(|| label_before_content(plain, &content));
        return Classification {
            kind: Kind::Chat,
            sender: Some(sender),
            content: Some(content),
            sender_label: label,
            ..Default::default()
        };
    }

    if let Some(captures) = NAME_TAG_CHAT.captures(plain) {
        let sender = captures.get(1).map(|m| m.as_str().to_owned()).unwrap_or_default();
        let content = captures.get(2).map(|m| m.as_str().to_owned()).unwrap_or_default();
        return chat_from_capture(plain, sender, content);
    }

    if plain.is_empty() || is_bridge_spam(plain) {
        Classification::of(Kind::Unknown)
    } else {
        Classification::of(Kind::Server)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_death_matchers() {
        assert!(death_matcher_count() > 90, "expected the full death table");
    }

    #[test]
    fn recognises_server_specific_deaths() {
        // Real lines from Constantiam that were being filed as "server".
        for (line, victim) in [
            ("RapzyS became epstein'd", "RapzyS"),
            ("RapzyS comitted sudoku", "RapzyS"),
            ("RapzyS killed themselves", "RapzyS"),
            ("RapzyS thought they could swim forever", "RapzyS"),
            ("5g4i fell into the void", "5g4i"),
            ("0boz was pushed off a high place by RapzyS", "0boz"),
            ("postironyc took 3 heads at once", "postironyc"),
            ("postironyc was burnt to a crisp", "postironyc"),
            ("xwonowx was burnt to a crisp", "xwonowx"),
            ("ItzPenguinGG thought lava was a hot tub", "ItzPenguinGG"),
            // Apostrophe has to survive regex escaping.
            ("ItzPenguinGG was knocked by a bunch of Creaking's", "ItzPenguinGG"),
            (
                "NotBananaBread blew up! They were playing around with an end-crystal!",
                "NotBananaBread",
            ),
        ] {
            let result = classify(line, false, false, None);
            assert_eq!(result.kind, Kind::Death, "should be a death: {line}");
            assert_eq!(result.subject.as_deref(), Some(victim), "wrong victim: {line}");
        }
    }

    #[test]
    fn captures_the_killer() {
        for (line, victim, killer) in [
            ("Notch was slain by Herobrine", "Notch", "Herobrine"),
            ("Notch was shot by Herobrine", "Notch", "Herobrine"),
            ("0boz was pushed off a high place by RapzyS", "0boz", "RapzyS"),
            // Vanilla's "slain by %2$s using %3$s" would credit "dwbb somehow"
            // here; the custom template has to win. Weapon names carry unicode
            // and brackets, which must not disturb the killer capture.
            (
                "NotBananaBread was slain by dwbb somehow using [𝙻𝚎𝚟𝙹𝚘𝚎´𝚜 𝙴𝚖𝚙𝚒𝚛𝚎 𝚂𝚠𝚘𝚛𝚍]",
                "NotBananaBread",
                "dwbb",
            ),
            // 2b2t victim-first + killer-first
            ("Steve was assassinated by Alex.", "Steve", "Alex"),
            ("Alex assassinated Steve with Netherite Sword", "Steve", "Alex"),
            ("Alex terminated Steve using Netherite Axe", "Steve", "Alex"),
            // 6b6t
            ("Steve was torn to shreds by Alex using Diamond Sword", "Steve", "Alex"),
        ] {
            let result = classify(line, false, false, None);
            assert_eq!(result.kind, Kind::Death, "{line}");
            assert_eq!(result.subject.as_deref(), Some(victim), "{line}");
            assert_eq!(result.killer.as_deref(), Some(killer), "{line}");
        }
    }

    #[test]
    fn deaths_with_no_killer_have_none() {
        for line in ["Notch fell from a high place", "RapzyS comitted sudoku"] {
            let result = classify(line, false, false, None);
            assert_eq!(result.kind, Kind::Death, "{line}");
            assert_eq!(result.killer, None, "nothing killed them: {line}");
        }
    }

    #[test]
    fn fighting_bystander_is_not_credited_with_the_kill() {
        // The anvil did the killing here; the named player was just nearby.
        let result = classify("Notch was squashed by a falling anvil while fighting Herobrine", false, false, None);
        assert_eq!(result.kind, Kind::Death);
        assert_eq!(result.killer, None, "a bystander must not be credited");
    }

    #[test]
    fn server_motd_is_not_a_death() {
        // These sit next to the death lines in the same feed and must not match.
        for line in [
            "Welcome to Constantiam!",
            "Player discretion is advised.",
            "This server is intended for mature audience.",
        ] {
            assert_eq!(classify(line, false, false, None).kind, Kind::Server, "{line}");
        }
    }

    #[test]
    fn twobtwot_and_sixbsixt_deaths() {
        for (line, victim) in [
            ("Steve fell to death.", "Steve"),
            ("Steve suicide bombed with an end crystal", "Steve"),
            ("Steve died.", "Steve"),
            ("Steve was utterly destroyed by Alex using Bow", "Steve"),
        ] {
            let result = classify(line, false, false, None);
            assert_eq!(result.kind, Kind::Death, "{line}");
            assert_eq!(result.subject.as_deref(), Some(victim), "{line}");
        }
    }

    #[test]
    fn classifies_deaths_from_flat_text() {
        let result = classify("Notch was slain by Herobrine", false, false, None);
        assert_eq!(result.kind, Kind::Death);
        assert_eq!(result.subject.as_deref(), Some("Notch"));
    }

    #[test]
    fn classifies_join_and_leave() {
        assert_eq!(classify("Notch joined the game", false, false, None).kind, Kind::Join);
        assert_eq!(classify("Notch left the game", false, false, None).kind, Kind::Leave);
        // 6b6t /connectionmsgs
        assert_eq!(classify("Notch joined.", false, false, None).kind, Kind::Join);

        // UneasyVanilla capitalises the verb; the tab list is not the only
        // source of join/leave there, chat is.
        let joined = classify("Sfxm Joined.", false, false, None);
        assert_eq!(joined.kind, Kind::Join);
        assert_eq!(joined.subject.as_deref(), Some("Sfxm"));
        let left = classify("Sfxm Left.", false, false, None);
        assert_eq!(left.kind, Kind::Leave);
        assert_eq!(left.subject.as_deref(), Some("Sfxm"));
        // A player typing it is still chat, not a presence event.
        assert_eq!(classify("Sfxm Joined.", true, false, None).kind, Kind::Chat);
        let quit = classify("jobless quit", false, false, None);
        assert_eq!(quit.kind, Kind::Leave);
        assert_eq!(quit.subject.as_deref(), Some("jobless"));
        assert_eq!(classify("Senki4444 quit.", false, false, None).kind, Kind::Leave);
    }

    #[test]
    fn leave_is_not_mistaken_for_a_death() {
        assert_eq!(classify("Notch left the game", false, false, None).kind, Kind::Leave);
    }

    #[test]
    fn player_packets_are_always_chat() {
        // Somebody typing "x joined the game" must not register as a join.
        let result = classify("Notch joined the game", true, false, None);
        assert_eq!(result.kind, Kind::Chat);
    }

    #[test]
    fn advancements_are_recognised() {
        let result = classify("Notch has made the advancement [Eye Spy]", false, false, None);
        assert_eq!(result.kind, Kind::Advancement);
    }

    #[test]
    fn system_formatted_chat_is_chat_not_server() {
        // Constantiam sends player chat as a system message in this shape; it
        // was being recorded as "server" and losing the sender entirely.
        let result = classify("<AIuminium> columbian cities are quite safe", false, false, None);
        assert_eq!(result.kind, Kind::Chat);
        assert_eq!(result.sender.as_deref(), Some("AIuminium"));
        assert_eq!(result.content.as_deref(), Some("columbian cities are quite safe"));
    }

    #[test]
    fn vanillaplus_rank_tags_are_chat() {
        let ranked = classify(
            "[ʙᴏᴏsᴛᴇʀ] <w1shlol> @jidion come get keywalks",
            false,
            false,
            None,
        );
        assert_eq!(ranked.kind, Kind::Chat);
        assert_eq!(ranked.sender.as_deref(), Some("w1shlol"));
        assert_eq!(ranked.content.as_deref(), Some("@jidion come get keywalks"));

        let paradise = classify(
            "[🌴ᴘᴀʀᴀᴅɪsᴇ] <ManuelG_MPG> 2 queue with 108/110 online",
            false,
            false,
            None,
        );
        assert_eq!(paradise.kind, Kind::Chat);
        assert_eq!(paradise.sender.as_deref(), Some("ManuelG_MPG"));

        let clan = classify("[ᴇɢɪʀʟ] <StyAway> Selling tokens", false, false, None);
        assert_eq!(clan.kind, Kind::Chat);
        assert_eq!(clan.sender.as_deref(), Some("StyAway"));

        let phantom = classify(
            "[ᴘʜᴀɴᴛᴏᴍ] <TheNightAgent> Selling kits 4ing per /trade",
            false,
            false,
            None,
        );
        assert_eq!(phantom.kind, Kind::Chat);
        assert_eq!(phantom.sender.as_deref(), Some("TheNightAgent"));

        let dotted = classify("<.Hightierthreeee> w1shlol is a qd", false, false, None);
        assert_eq!(dotted.kind, Kind::Chat);
        assert_eq!(dotted.sender.as_deref(), Some(".Hightierthreeee"));

        assert_eq!(
            classify(
                "[Vanilla+] The Crystal Zone will reset in 30s! This may cause a brief lag.",
                false,
                false,
                None,
            )
            .kind,
            Kind::Server
        );
    }

    #[test]
    fn hazey_ranked_angle_chat_is_chat() {
        let ranked = classify("<[M] SawgerGG> » want to learn how to dupe?", false, false, None);
        assert_eq!(ranked.kind, Kind::Chat);
        assert_eq!(ranked.sender.as_deref(), Some("SawgerGG"));
        assert_eq!(ranked.sender_label.as_deref(), Some("<[M] SawgerGG>"));
        assert_eq!(
            ranked.content.as_deref(),
            Some("want to learn how to dupe?")
        );

        let whisper = classify("[m1rka114] <[M] m1rka114> » hello", false, false, None);
        assert_eq!(whisper.kind, Kind::Chat);
        assert_eq!(whisper.sender.as_deref(), Some("m1rka114"));
        assert_eq!(whisper.sender_label.as_deref(), Some("[m1rka114] <[M] m1rka114>"));

        let outer = classify("[_Banzz] <_Banzz> anything happening", false, false, None);
        assert_eq!(outer.kind, Kind::Chat);
        assert_eq!(outer.sender.as_deref(), Some("_Banzz"));
        assert_eq!(outer.sender_label.as_deref(), Some("[_Banzz] <_Banzz>"));
    }

    #[test]
    fn tagged_rank_chat_and_tablist() {
        let ranked = classify(
            "[8] [MVP] DesBob [slime] » ty though",
            false,
            false,
            None,
        );
        assert_eq!(ranked.kind, Kind::Chat);
        assert_eq!(ranked.sender.as_deref(), Some("DesBob"));
        assert_eq!(ranked.sender_label.as_deref(), Some("[8] [MVP] DesBob [slime]"));

        let sixbsixt = classify(
            "[Prime] TheGroupProject » Ranked Players Only!",
            false,
            false,
            None,
        );
        assert_eq!(sixbsixt.kind, Kind::Chat);
        assert_eq!(sixbsixt.sender.as_deref(), Some("TheGroupProject"));
        assert_eq!(sixbsixt.sender_label.as_deref(), Some("[Prime] TheGroupProject"));

        let plain = classify("jointedx21 » bro i keep getting killed", false, false, None);
        assert_eq!(plain.kind, Kind::Chat);
        assert_eq!(plain.sender.as_deref(), Some("jointedx21"));

        let flake = classify("❄ santiahre » gg", false, false, None);
        assert_eq!(flake.kind, Kind::Chat);
        assert_eq!(flake.sender.as_deref(), Some("santiahre"));
        assert_eq!(flake.content.as_deref(), Some("gg"));
        assert_eq!(flake.sender_label.as_deref(), Some("❄ santiahre"));

        let join = classify("[+] chjn12", false, false, None);
        assert_eq!(join.kind, Kind::Join);
        assert_eq!(join.subject.as_deref(), Some("chjn12"));

        let leave = classify("[✖] ka1gamer107", false, false, None);
        assert_eq!(leave.kind, Kind::Leave);
        assert_eq!(leave.subject.as_deref(), Some("ka1gamer107"));
    }

    #[test]
    fn minewind_rank_dot_chat_is_chat() {
        let ranked = classify("Neos.Grave: dude n", false, false, None);
        assert_eq!(ranked.kind, Kind::Chat);
        assert_eq!(ranked.sender.as_deref(), Some("Grave"));
        assert_eq!(ranked.content.as_deref(), Some("dude n"));

        let welcome = classify("Welcome hornet68!", false, false, None);
        assert_eq!(welcome.kind, Kind::Join);
        assert_eq!(welcome.subject.as_deref(), Some("hornet68"));

        assert_eq!(
            classify("Price Check: PC: Wither 2 costs 3s", false, false, None).kind,
            Kind::Server
        );
        for line in [
            "Explainer: Magma Orb (Spell): Fires a slow homing shot that explodes on impact",
            "DebugMenu: Your luck is above average today",
        ] {
            let result = classify(line, false, false, None);
            assert_eq!(result.kind, Kind::Server, "{line}");
            assert_eq!(result.sender, None, "{line}");
        }
    }

    #[test]
    fn prefixed_chat_survives_rank_decoration() {
        let result = classify("Raekuuro ⛏ > nice", false, false, None);
        assert_eq!(result.kind, Kind::Chat);
        assert_eq!(result.sender.as_deref(), Some("Raekuuro"));
        assert_eq!(result.content.as_deref(), Some("nice"));
    }

    #[test]
    fn rempolon_rank_arrow_chat_is_chat() {
        for (line, sender, content, label) in [
            (
                "ᴘʟᴀʏᴇʀ+ Cloudcon ➤ man..",
                "Cloudcon",
                "man..",
                "ᴘʟᴀʏᴇʀ+ Cloudcon",
            ),
            (
                "ᴘʟᴀʏᴇʀ RoyalBluYT ➤ god i hate powdered snow",
                "RoyalBluYT",
                "god i hate powdered snow",
                "ᴘʟᴀʏᴇʀ RoyalBluYT",
            ),
            (
                "ULTRA GrimVoidX ➤ yoo chill mate",
                "GrimVoidX",
                "yoo chill mate",
                "ULTRA GrimVoidX",
            ),
            (
                "GODLIKE BingsChungus ➤ 5k",
                "BingsChungus",
                "5k",
                "GODLIKE BingsChungus",
            ),
            (
                "[mitropolzka] ᴘʟᴀʏᴇʀ mitropolzka ➤ @GrimVoidX language",
                "mitropolzka",
                "@GrimVoidX language",
                "[mitropolzka] ᴘʟᴀʏᴇʀ mitropolzka",
            ),
        ] {
            let result = classify(line, false, false, None);
            assert_eq!(result.kind, Kind::Chat, "{line}");
            assert_eq!(result.sender.as_deref(), Some(sender), "{line}");
            assert_eq!(result.content.as_deref(), Some(content), "{line}");
            assert_eq!(result.sender_label.as_deref(), Some(label), "{line}");
        }

        assert_eq!(
            classify("Get from store.rempolon.eu", false, false, None).kind,
            Kind::Server
        );
    }

    #[test]
    fn quoted_chat_beats_death_matching() {
        // Somebody typing a death message must stay chat.
        let result = classify("<Notch> Herobrine was slain by Notch", false, false, None);
        assert_eq!(result.kind, Kind::Chat);
    }

    #[test]
    fn server_notices_are_not_chat() {
        let result = classify("Use support@constantiam.net to contact the admin", false, false, None);
        assert_eq!(result.kind, Kind::Server);
    }

    #[test]
    fn strips_and_rejects_tablist_dummy_names() {
        assert_eq!(
            clean_player_name("§1§r§o§f§0§8§c§r"),
            None,
            "colour-code dummy should drop"
        );
        assert_eq!(clean_player_name("§4§3§3§k§e§6§8§3"), None);
        assert_eq!(clean_player_name("§b§0§6§m§o§2§0§o"), None);
        assert_eq!(clean_player_name("0boz").as_deref(), Some("0boz"));
        assert_eq!(clean_player_name("§a0boz").as_deref(), Some("0boz"));
        assert_eq!(clean_player_name("renniethe_elf").as_deref(), Some("renniethe_elf"));
        assert_eq!(
            clean_player_name(".SlyNebula3267").as_deref(),
            Some(".SlyNebula3267")
        );
        assert_eq!(clean_player_name("bob").as_deref(), Some("bob"));
    }

    #[test]
    fn formatted_dummy_join_leave_has_no_subject() {
        let leave = classify("§4§3§3§k§e§6§8§3 left the game", false, false, None);
        assert_eq!(leave.kind, Kind::Leave);
        assert_eq!(leave.subject, None, "NPC leftover codes must not be a player");

        let junk = classify("§1§r§o§f§0§8§c§r left the game", false, false, None);
        assert_eq!(junk.kind, Kind::Leave);
        assert_eq!(junk.subject, None);

        let real = classify("renniethe_elf left the game", false, false, None);
        assert_eq!(real.kind, Kind::Leave);
        assert_eq!(real.subject.as_deref(), Some("renniethe_elf"));
    }

    #[test]
    fn cutesmp_system_spam_is_not_bridged() {
        for line in [
            "Unknown command.",
            "Remember to /vote to get free rewards",
            "You still have daily quests to complete !",
            "Quests\nYou still have daily quests to complete !",
            "[ WELCOME ] 0boz to CuteSMP",
            "You're protected from attack while your inventory is empty",
        ] {
            let result = classify(line, false, false, None);
            assert_eq!(result.kind, Kind::Unknown, "{line}");
            assert_eq!(result.sender, None, "{line}");
        }

        let quests = classify("Quests: You still have daily quests to complete !", false, false, None);
        assert_ne!(quests.kind, Kind::Chat);
        assert_eq!(quests.sender, None);
    }

    #[test]
    fn cutesmp_player_chat_is_clean() {
        let ranked = classify(
            "[entityexisting] Fairy entityexisting [Axolotl]: welcome!",
            false,
            false,
            None,
        );
        assert_eq!(ranked.kind, Kind::Chat);
        assert_eq!(ranked.sender.as_deref(), Some("entityexisting"));
        assert_eq!(ranked.content.as_deref(), Some("welcome!"));
        assert_eq!(ranked.sender_label, None);

        let ad = classify(
            ".lenalovesyogurt [i] come get your free ticket! /warp shop",
            false,
            false,
            None,
        );
        assert_eq!(ad.kind, Kind::Chat);
        assert_eq!(ad.sender.as_deref(), Some(".lenalovesyogurt"));
        assert!(ad.content.as_deref().unwrap_or("").contains("come get your free ticket"));
    }

    #[test]
    fn real_death_is_not_spam() {
        let result = classify("Accolori fell from a high place", false, false, None);
        assert_eq!(result.kind, Kind::Death);
        assert_eq!(result.subject.as_deref(), Some("Accolori"));
    }

    #[test]
    fn server_notices_are_never_deaths() {
        // A discovered-template harvester once filed this 6b6t advert as a death
        // template, so every airing of it was stored as `6b6t` dying — and the
        // feed rendered it as a player line with a head.
        let notice = "---------------------------
6b6t has many commands, but do you know them all? Check out the 6b6t Commands page to see a full list.
---------------------------";
        let result = classify(notice, false, false, None);
        assert_eq!(result.kind, Kind::Server, "{notice}");
        assert_eq!(result.subject, None);
    }

    #[test]
    fn sixbsixt_server_brand_is_not_a_player() {
        // Live 6b6t rows: the server account uses the same `»` format as players,
        // so classify was storing kind=c with sender 6Builders6Tools / 6b6t.
        for line in [
            "6b6t » ==pal==",
            "[6b6t] Keep the Dupe Event running - buy a rank from the 6b6t Shop",
            "6b6t: Join our Discord with /discord while waiting.",
            "6b6t will soon come back online. Join our Discord with /discord while waiting.",
            "Player StarTrap_exe purchased the Elite Rank and extended the Dupe Event by 1 hour.",
            "Connecting to the server...",
        ] {
            let result = classify(line, false, false, None);
            assert_eq!(result.kind, Kind::Server, "{line}");
            assert_eq!(result.sender, None, "{line}");
        }

        // 6Builders6Tools is a player account that posts with a rank prefix,
        // not the server brand: its lines belong in the feed as chat.
        for line in [
            "[APE 🍰] 6Builders6Tools » Who wants a signed book?",
            "6Builders6Tools » Keep DUPING and BUYING RANKS!!!",
        ] {
            let result = classify(line, false, false, None);
            assert_eq!(result.kind, Kind::Chat, "{line}");
            assert_eq!(result.sender.as_deref(), Some("6Builders6Tools"), "{line}");
        }

        let ranked = classify(
            "[Prime] TheGroupProject » Ranked Players Only!",
            false,
            false,
            None,
        );
        assert_eq!(ranked.kind, Kind::Chat);
        assert_eq!(ranked.sender.as_deref(), Some("TheGroupProject"));

        let plain = classify("jointedx21 » bro i keep getting killed", false, false, None);
        assert_eq!(plain.kind, Kind::Chat);
        assert_eq!(plain.sender.as_deref(), Some("jointedx21"));
    }
}

#[cfg(test)]
mod restart_countdown_tests {
    use super::restart_countdown_secs;

    #[test]
    fn parses_every_form_these_servers_actually_send() {
        // 6b6t
        assert_eq!(restart_countdown_secs("Server restarts in 1s"), Some(1));
        assert_eq!(restart_countdown_secs("Server restarts in 10s"), Some(10));
        assert_eq!(restart_countdown_secs("Server restarts in 1m"), Some(60));
        assert_eq!(restart_countdown_secs("Server restarts in 1m 15s"), Some(75));
        assert_eq!(
            restart_countdown_secs("This proxy will restart in 10m. You will be reconnected automatically."),
            Some(600)
        );
        assert_eq!(
            restart_countdown_secs("This proxy is restarting now. You will be reconnected automatically."),
            Some(0)
        );
        // 2b2t
        assert_eq!(restart_countdown_secs("Server restarting in 1 second..."), Some(1));
        assert_eq!(restart_countdown_secs("Server restarting in 10 seconds..."), Some(10));
        assert_eq!(restart_countdown_secs("Server restarting in 2 minutes..."), Some(120));
        // vanillaplus
        assert_eq!(restart_countdown_secs("RESTART | Server restarting in 16s"), Some(16));
        assert_eq!(restart_countdown_secs("RESTART | Server restarting in 1m 00s"), Some(60));
        assert_eq!(restart_countdown_secs("The server will restart in 30s."), Some(30));
    }

    #[test]
    fn ignores_lines_that_are_not_countdowns() {
        assert_eq!(restart_countdown_secs("Herobrine joined the game"), None);
        assert_eq!(restart_countdown_secs("6B6T RESTARTING !!!!!"), None);
        assert_eq!(restart_countdown_secs("=restart"), None);
        // A player grumbling about restarts must never clear the tab list.
        assert_eq!(restart_countdown_secs("$addfaq qbasty like restart the server"), None);
    }
}
