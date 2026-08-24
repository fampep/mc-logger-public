/**
 * One-off: UneasyVanilla `Name Left.` was auto-added as a death template.
 * Teleport requests were the same class of mistake. This files them back.
 *
 * Run: npx tsx scripts/unfile-nondeaths.ts --apply
 */
import { closeAllPools, poolFor } from '../src/db.js';
import { config } from '../src/config.js';

const apply = process.argv.includes('--apply');

for (const server of config.servers) {
  const pool = poolFor(server.key);

  const leaves = await pool.query<{ n: number }>(
    `SELECT count(*)::int AS n FROM chat_messages
      WHERE kind = 'death' AND plain_text LIKE '% Left.'`,
  );
  const teleports = await pool.query<{ n: number }>(
    `SELECT count(*)::int AS n FROM chat_messages
      WHERE kind = 'death'
        AND (plain_text ILIKE '%wants to teleport to you%'
          OR plain_text ILIKE '%requests teleportation to you%'
          OR plain_text ILIKE '%ᴛᴇʟᴇᴘᴏʀᴛ%')`,
  );

  const leaveN = leaves.rows[0]?.n ?? 0;
  const tpN = teleports.rows[0]?.n ?? 0;
  if (leaveN === 0 && tpN === 0) continue;

  console.log(`${server.key}: ${leaveN} leave(s) filed as death, ${tpN} teleport(s) filed as death`);
  if (!apply) continue;

  if (leaveN > 0) {
    const result = await pool.query(
      `UPDATE chat_messages
          SET kind = 'leave',
              killer_name = NULL,
              subject_name = regexp_replace(plain_text, ' Left\\.$', '')
        WHERE kind = 'death' AND plain_text LIKE '% Left.'`,
    );
    console.log(`  unfiled ${result.rowCount} leave(s)`);
  }
  if (tpN > 0) {
    const result = await pool.query(
      `UPDATE chat_messages
          SET kind = 'server', subject_name = NULL, killer_name = NULL
        WHERE kind = 'death'
          AND (plain_text ILIKE '%wants to teleport to you%'
            OR plain_text ILIKE '%requests teleportation to you%'
            OR plain_text ILIKE '%ᴛᴇʟᴇᴘᴏʀᴛ%')`,
    );
    console.log(`  unfiled ${result.rowCount} teleport(s)`);
  }
}

console.log(apply ? 'Done.' : 'Re-run with --apply to write.');
await closeAllPools();
