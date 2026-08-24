# server-seeker

ServerSeekerV2-style Minecraft **Java edition** discovery for mc-logger — **automatic IP + domain scanning** with live Discord posting.

## How live fetch works

```
watch loop (every SEEKER_WATCH_INTERVAL seconds):
  1. masscan → status ping open :25565 ports → Postgres
  2. domain DNS resolve → status ping each hostname → Postgres
  3. optional CIDR pass
  4. rescan known DB servers
  → mc-discord-bot seeker-bridge polls Postgres every 5s → Discord embeds
```

One-time setup: `/seeker bridge set` in Discord to pick a channel. After that, scans → DB → bridge posts automatically.

## Modes

| Mode | Env | Description |
| --- | --- | --- |
| **watch** (default) | `SEEKER_MODE=watch` | Continuous masscan + domains + rescan loop |
| **scanner** | `SEEKER_MODE=scanner` | Single masscan pass |
| **rescanner** | `SEEKER_MODE=rescanner` | Re-ping DB servers once |
| **cidr** | `SEEKER_MODE=cidr` | Direct IP range ping |

## Config (.env)

```env
SEEKER_DATABASE_URL=postgres://user:pass@localhost/constantiam
SEEKER_MODE=watch
SEEKER_WATCH_INTERVAL=3600

SEEKER_MASSCAN_CONFIG=masscan.conf
SEEKER_MASSCAN_SUDO=true

SEEKER_SEEDS=purityvanilla.com,play.vanillaplus.net
SEEKER_QUERIES=vanillaplus,vanilla plus
SEEKER_I_UNDERSTAND=true
```

- `SEEKER_SEEDS` or `SEEKER_DOMAINS` — comma-separated hostnames
- `SEEKER_QUERIES` — MOTD/version keyword filter (empty = store all)
- `SEEKER_I_UNDERSTAND=true` — required for masscan/CIDR on public IPs

## Build & run

```bash
cd tools/server-seeker
cp masscan.conf.example masscan.conf   # edit range to something safe
cp .env.example .env
cargo run --release -- --mode watch --i-understand
```

## VPS deploy

```bash
./deploy/seeker-install.sh
journalctl -u server-seeker -f
```

Uses `Type=simple` systemd service (not timer) for continuous watch loop.

## Discord bridge

1. Set `SEEKER_DATABASE_URL` in `mc-discord-bot/.env` (same DB)
2. Restart bot — bridge starts automatically
3. `/seeker bridge set` — pick channel (once)
4. New `discovered_servers` rows appear as embeds within ~5s

## Legal

Only scan IP ranges you own or have permission to probe. Use a small `masscan.conf` range for testing.
