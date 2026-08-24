/**
 * Applies the current `online_now` definition from the logger's schema to every
 * configured database.
 *
 * azalea-bot runs schema.sql on each start, so this is only needed to fix a
 * running deployment without restarting the loggers. The statement is read out
 * of schema.sql rather than copied, so this cannot drift from it.
 *
 * Idempotent: CREATE OR REPLACE VIEW. Run with `npx tsx scripts/sync-online-view.ts`.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { config } from '../src/config.js';
import { closeAllPools, poolFor } from '../src/db.js';

const here = dirname(fileURLToPath(import.meta.url));
const schemaPath = resolve(here, '../../azalea-bot/src/schema.sql');
const schema = await readFile(schemaPath, 'utf8');

const statement = /CREATE OR REPLACE VIEW online_now AS[\s\S]*?;/.exec(schema)?.[0];
if (!statement) {
  console.error(`Could not find the online_now view in ${schemaPath}`);
  process.exit(1);
}

console.log(`Applying from ${schemaPath}:\n`);
console.log(statement, '\n');

for (const server of config.servers) {
  try {
    const pool = poolFor(server.key);
    const before = await pool.query<{ count: number }>('SELECT count(*) AS count FROM online_now');
    await pool.query(statement);
    const after = await pool.query<{ count: number }>('SELECT count(*) AS count FROM online_now');
    console.log(
      `${server.label.padEnd(12)} online_now ${String(before.rows[0]!.count).padStart(4)} -> ${String(after.rows[0]!.count).padStart(4)}`,
    );
  } catch (error) {
    console.log(`${server.label.padEnd(12)} FAILED: ${(error as Error).message}`);
  }
}

await closeAllPools();
