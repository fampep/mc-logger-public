//! Player-head rendering URLs used by Discord presentation code.
//!
//! Addressed by name, not uuid: crafthead and mc-heads both render straight
//! from a bare username, so there is no lookup, no cache, and no background
//! resolution task standing between a name and a render URL anymore. A
//! cracked account sharing a name with someone else's real account can
//! render the wrong skin as a result — a deliberate trade for never having a
//! render lag behind or block on a uuid that has not been seen yet.

use crate::config::Config;

/// Names are only worth a render if they could be usernames at all.
fn looks_like_username(name: &str) -> bool {
    !name.is_empty()
        && name.chars().count() <= 16
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Head URL for `name`, or `None` when it cannot be a real username (a
/// server brand or a decorated label, say — there is no player behind it and
/// so no skin to show).
pub fn url_for(name: &str, config: &Config) -> Option<String> {
    looks_like_username(name).then(|| config.head_url_name(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_real_username_shapes_are_worth_a_lookup() {
        assert!(looks_like_username("Notch"));
        assert!(looks_like_username("a_long_name_16ch"));
        assert!(!looks_like_username(""));
        assert!(!looks_like_username("this_name_is_far_too_long"));
        // Bedrock prefixes and decorated labels cannot resolve to a player row.
        assert!(!looks_like_username(".BedrockGuy"));
        assert!(!looks_like_username("[APE] 6Builders6Tools"));
    }

    #[test]
    fn renders_are_addressed_by_name_in_each_services_own_format() {
        use crate::config::{body_url_for_name, head_url_for_name};

        assert_eq!(
            head_url_for_name("Notch", 64),
            "https://crafthead.net/helm/Notch/64"
        );
        assert_eq!(
            body_url_for_name("Notch"),
            "https://mc-heads.net/body/Notch"
        );
    }
}
