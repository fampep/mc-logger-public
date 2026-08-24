/**
 * Read-only audit of who `online_now` claims is online, and whether the logger
 * was actually connected when it last saw them.
 *
 * Run with `npx tsx scripts/presence-audit.ts`. Touches nothing.
 */
import { config } from '../src/config.js';
import { closeAllPools, poolFor } from '../src/db.js';

interface Row {
  player_name: string;
  occurred_at: Date;
  event_type: string;
  session_open: boolean | null;
  end_reason: string | null;
}

const LATEST_PRESENCE = `
  WITH latest AS (
    SELECT DISTINCT ON (server_host, lower(player_name))
           server_host, player_name, occurred_at, event_type, session_id
    FROM player_events
    ORDER BY server_host, lower(player_name), occurred_at DESC, id DESC
  )
  SELECT l.player_name, l.occurred_at, l.event_type,
         (s.ended_at IS NULL) AS session_open, s.end_reason
  FROM latest l
  LEFT JOIN sessions s ON s.id = l.session_id
  WHERE l.event_type <> 'leave'
  ORDER BY l.occurred_at
`;

function hours(date: Date): string {
  return `${((Date.now() - date.getTime()) / 3_600_000).toFixed(1)}h ago`;
}

for (const server of config.servers) {
  console.log(`\n=== ${server.label} (${server.key})`);

  try {
    const pool = poolFor(server.key);

    const sessions = await pool.query<{ open: number; latest: Date | null; reason: string | null }>(
      `SELECT count(*) FILTER (WHERE ended_at IS NULL) AS open,
              max(started_at) AS latest,
              (SELECT end_reason FROM sessions ORDER BY started_at DESC LIMIT 1) AS reason
       FROM sessions`,
    );
    const { open, latest, reason } = sessions.rows[0]!;
    console.log(
      `sessions: ${open} open, newest started ${latest ? hours(latest) : 'never'}${reason ? ` (last reason: ${reason})` : ''}`,
    );

    const onlineNow = await pool.query<{ count: number }>('SELECT count(*) AS count FROM online_now');
    const rows = (await pool.query<Row>(LATEST_PRESENCE)).rows;
    const stale = rows.filter((row) => row.session_open === false);
    const live = rows.filter((row) => row.session_open === true);

    console.log(`online_now says: ${onlineNow.rows[0]!.count}`);
    console.log(`  from an open session (plausible): ${live.length}`);
    console.log(`  from a CLOSED session (stale):    ${stale.length}`);

    for (const row of stale.slice(0, 5)) {
      console.log(`    ${row.player_name.padEnd(18)} last ${row.event_type.padEnd(8)} ${hours(row.occurred_at)}`);
    }
    if (stale.length > 5) console.log(`    …and ${stale.length - 5} more`);
  } catch (error) {
    console.log(`unreachable: ${(error as Error).message}`);
  }
}

await closeAllPools();
