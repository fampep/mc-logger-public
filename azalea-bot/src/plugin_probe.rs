//! In-game server plugin discovery — same techniques hacked clients use.
//!
//! Meteor Client's `.server plugins` (see their `ServerCommand.java`) works by:
//! 1. Parsing the brigadier command tree sent on login for namespaced commands
//!    like `essentials:home` → plugin `essentials`.
//! 2. Sending a tab-complete request for a version alias (`version `, etc.) and
//!    reading plugin names from the suggestion list.
//!
//! We also try `/plugins` and `/pl` in chat when servers leave those open.
//!
//! PluginGuard-style protection usually strips the command tree and blocks `/plugins`,
//! but many servers still send a `minecraft:register` custom payload listing every
//! plugin message channel (e.g. `luckperms:update`, `worldedit:cui`). That packet
//! is the main extra leak surface — there is no separate "hide plugins" packet to
//! bypass, only data the server chooses not to send.

use std::collections::BTreeSet;

/// Bukkit version-command aliases Meteor checks for tab-complete plugin lists.
pub const VERSION_ALIASES: &[&str] = &[
    "version",
    "ver",
    "about",
    "bukkit:version",
    "bukkit:ver",
    "bukkit:about",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginSource {
    CommandTree,
    TabComplete,
    Chat,
    /// Names from a server `minecraft:register` payload (null-separated channel list).
    RegisterChannel,
    /// Inferred from any other custom payload channel name the server uses.
    PluginChannel,
}

impl PluginSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CommandTree => "command_tree",
            Self::TabComplete => "tab_complete",
            Self::Chat => "chat",
            Self::RegisterChannel => "register_channel",
            Self::PluginChannel => "plugin_channel",
        }
    }
}

#[derive(Debug, Clone)]
pub struct DetectedPlugin {
    pub name: String,
    pub sources: BTreeSet<&'static str>,
}

/// Namespaces from brigadier command node names (Meteor's `commandTreePlugins`).
pub fn plugins_from_command_names(names: impl IntoIterator<Item = impl AsRef<str>>) -> (Vec<String>, Option<String>) {
    let mut plugins = BTreeSet::new();
    let mut version_alias = None;

    for name in names {
        let name = name.as_ref();

        if VERSION_ALIASES.contains(&name) && version_alias.is_none() {
            version_alias = Some(name.to_owned());
        }

        if let Some(namespace) = namespace_of_command(name) {
            plugins.insert(namespace);
        }
    }

    (plugins.into_iter().collect(), version_alias)
}

/// Plugin names from a tab-complete response to `version ` / similar.
pub fn plugins_from_tab_suggestions(texts: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut out = BTreeSet::new();
    for text in texts {
        if let Some(name) = parse_tab_plugin_line(&text) {
            out.insert(name);
        }
    }
    out.into_iter().collect()
}

/// Parse `/plugins` style chat output.
pub fn plugins_from_chat(text: &str) -> Vec<String> {
    let mut out = BTreeSet::new();
    let lower = text.to_lowercase();

    // "Plugins (12): A, B, C" or "Server Plugins (12): ..."
    if lower.contains("plugin") {
        if let Some(rest) = text.split(':').nth(1) {
            for part in rest.split([',', ';']) {
                if let Some(name) = clean_plugin_token(part) {
                    out.insert(name);
                }
            }
        }
        for line in text.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('-') || trimmed.starts_with('•') {
                if let Some(name) = clean_plugin_token(trimmed.trim_start_matches(['-', '•', '*', ' '])) {
                    out.insert(name);
                }
            }
        }
    }

    out.into_iter().collect()
}

/// Parse the payload of `minecraft:register` / `minecraft:unregister`.
pub fn parse_register_channels(data: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(data)
        .split('\0')
        .filter(|channel| !channel.is_empty())
        .map(str::trim)
        .filter(|channel| !channel.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Map a plugin message channel to a human plugin name (`worldedit:cui` → WorldEdit).
pub fn plugin_from_channel(channel: &str) -> Option<String> {
    let channel = channel.trim();
    if channel.is_empty() {
        return None;
    }

    match channel {
        "WECUI" | "worldedit:cui" => return Some("WorldEdit".to_string()),
        "BungeeCord" | "bungeecord:main" => return Some("BungeeCord".to_string()),
        _ => {}
    }

    if channel.starts_with("minecraft:") {
        return None;
    }

    if let Some((namespace, _)) = channel.split_once(':') {
        if is_ignored_namespace(namespace) {
            return None;
        }
        return Some(normalize_plugin_name(namespace));
    }

    if channel.contains(':') {
        return None;
    }

    Some(normalize_plugin_name(channel))
}

pub fn plugins_from_register_channels(channels: impl IntoIterator<Item = impl AsRef<str>>) -> Vec<String> {
    let mut out = BTreeSet::new();
    for channel in channels {
        if let Some(name) = plugin_from_channel(channel.as_ref()) {
            out.insert(name);
        }
    }
    out.into_iter().collect()
}

pub fn merge_plugins(sources: &[(PluginSource, Vec<String>)]) -> Vec<DetectedPlugin> {
    let mut by_name: std::collections::BTreeMap<String, BTreeSet<&'static str>> =
        std::collections::BTreeMap::new();

    for (source, names) in sources {
        for name in names {
            by_name
                .entry(normalize_plugin_name(name))
                .or_default()
                .insert(source.as_str());
        }
    }

    by_name
        .into_iter()
        .map(|(name, sources)| DetectedPlugin { name, sources })
        .collect()
}

fn namespace_of_command(name: &str) -> Option<String> {
    let (namespace, _) = name.split_once(':')?;
    let namespace = namespace.trim();
    if namespace.is_empty() || namespace == "minecraft" || namespace == "bukkit" {
        return None;
    }
    Some(normalize_plugin_name(namespace))
}

fn parse_tab_plugin_line(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    // "WorldEdit version: 7.2.15" → WorldEdit
    let head = trimmed.split(" version").next()?.trim();
    clean_plugin_token(head)
}

fn clean_plugin_token(raw: &str) -> Option<String> {
    let mut token = raw.trim();
    token = token.trim_matches(['(', ')', '[', ']']);
    if token.is_empty() {
        return None;
    }
    // Drop trailing version segment: "WorldEdit v7.2"
    let name = token.split_whitespace().next()?.trim();
    if name.eq_ignore_ascii_case("plugins") || name.eq_ignore_ascii_case("server") {
        return None;
    }
    Some(normalize_plugin_name(name))
}

fn normalize_plugin_name(name: &str) -> String {
    name.trim().to_string()
}

fn is_ignored_namespace(namespace: &str) -> bool {
    matches!(
        namespace,
        "minecraft" | "bukkit" | "c" | "fml" | "forge" | "fabric" | "neoforge"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_chat_plugin_list() {
        let text = "Plugins (3): WorldEdit, Essentials, LuckPerms";
        let plugins = plugins_from_chat(text);
        assert!(plugins.contains(&"WorldEdit".to_string()));
        assert!(plugins.contains(&"Essentials".to_string()));
        assert_eq!(plugins.len(), 3);
    }

    #[test]
    fn parses_tab_version_line() {
        assert_eq!(
            parse_tab_plugin_line("WorldEdit version: 7.2.15").as_deref(),
            Some("WorldEdit")
        );
    }

    #[test]
    fn parses_register_payload() {
        let payload = b"luckperms:update\0worldedit:cui\0essentials:msg\0";
        let channels = parse_register_channels(payload);
        assert_eq!(channels.len(), 3);
        let plugins = plugins_from_register_channels(channels);
        assert!(plugins.contains(&"luckperms".to_string()));
        assert!(plugins.contains(&"WorldEdit".to_string()));
        assert!(plugins.contains(&"essentials".to_string()));
    }
}
