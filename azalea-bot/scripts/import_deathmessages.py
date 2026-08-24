"""Turns a DeathMessages plugin config into classifier templates.

9b9t (and Constantiam, which runs the same plugin) publish the exact list of
death messages they can send. Reading it beats discovering them one at a time:
every message is known up front rather than after the first player dies to it.

    python scripts/import_deathmessages.py PlayerDeathMessages.yml 9b9t

Placeholders map onto template slots by order of appearance, because the plugin
puts the killer first in some messages ("X blew up Y") and the victim first in
most. A message that repeats a placeholder is skipped: the template compiler
turns every occurrence into its own capture group, which would shift the
numbering and silently break victim and killer extraction.
"""

import json
import re
import sys
from pathlib import Path

VICTIM = {"%player%", "%player_display%"}
KILLER = {"%killer%", "%killer_display%", "%killer_type%"}
# Everything else is an item, a place or a number — never someone to credit.
OTHER = {
    "%weapon%", "%world%", "%world_environment%", "%x%", "%y%", "%z%",
    "%distance%", "%biome%", "%climbable%", "%block%", "%entity%", "%item%",
}
PLACEHOLDER = re.compile(r"%[a-z_]+%")

COLOUR = re.compile(r"&[0-9a-fk-orA-FK-OR]")
# Config-only prefixes that never reach chat.
MARKER = re.compile(r"^(?:PERMISSION(?:_KILLER)?\[[^\]]*\]|REGION\[[^\]]*\])+")
# "[base::hover::action]" renders as just the base text.
COMPONENT = re.compile(r"\[([^\[\]]*?)::[^\[\]]*?\]")


def clean(message: str) -> str:
    message = MARKER.sub("", message)
    message = COMPONENT.sub(lambda m: m.group(1), message)
    message = COLOUR.sub("", message)
    # A bare "base::hover" with no brackets renders as the base too.
    if "::" in message:
        message = message.split("::", 1)[0]
    return message.strip()


def messages_in(path: Path):
    """Every list item in the file, without needing a YAML parser.

    The config is only ever nested maps of string lists, so a line starting
    with "- " is a message and nothing else is.
    """
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line.startswith("- "):
            continue
        value = line[2:].strip()
        if value in ("[]", ""):
            continue
        # Strip one layer of YAML quoting, un-escaping doubled quotes.
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            quote = value[0]
            value = value[1:-1].replace(quote * 2, quote)
        yield value


def to_template(message: str):
    """(template, victim_slot, killer_slot) or None if it cannot be expressed."""
    found = PLACEHOLDER.findall(message)
    if not any(p in VICTIM for p in found):
        return None
    # A repeated placeholder would compile to two capture groups.
    if len(found) != len(set(found)):
        return None
    if any(p not in VICTIM and p not in KILLER and p not in OTHER for p in found):
        return None

    template = message
    victim = killer = None
    for index, placeholder in enumerate(dict.fromkeys(found), start=1):
        template = template.replace(placeholder, f"%{index}$s")
        if placeholder in VICTIM:
            victim = index
        elif placeholder in KILLER:
            killer = index

    # Slots are numbered by appearance, so a template can end up with the
    # weapon before the victim. That is fine — only which slot is which matters.
    return template, victim, killer


def main() -> int:
    if len(sys.argv) < 3:
        print(__doc__)
        return 2

    source = Path(sys.argv[1])
    group = sys.argv[2]
    target = Path(__file__).resolve().parent.parent / "src" / "custom_deaths.json"

    data = json.loads(target.read_text(encoding="utf-8"))
    existing = set()
    for key, entries in data.items():
        if key.startswith("_"):
            continue
        for entry in entries:
            existing.add(entry if isinstance(entry, str) else entry["template"])

    added, skipped = [], 0
    seen = set()
    for message in messages_in(source):
        cleaned = clean(message)
        if not cleaned:
            continue
        built = to_template(cleaned)
        if built is None:
            skipped += 1
            continue
        template, victim, killer = built
        if template in existing or template in seen:
            continue
        seen.add(template)

        # A plain string means victim in slot 1 and a killer inferred from the
        # "by %2$s" phrasing. Anything else has to name its slots.
        if victim == 1 and killer is None and "%2$s" not in template:
            added.append(template)
        elif victim == 1 and killer == 2 and "by %2$s" in template:
            added.append(template)
        else:
            entry = {"template": template, "victim": victim}
            if killer is not None:
                entry["killer"] = killer
            added.append(entry)

    data.setdefault(group, []).extend(added)
    with target.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")

    print(f"added {len(added)} template(s) to '{group}', skipped {skipped} unexpressible")
    print(f"'{group}' now has {len(data[group])}; {sum(len(v) for k, v in data.items() if not k.startswith('_'))} total")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
