import { closeAllPools, poolFor } from '../src/db.js';
import { config } from '../src/config.js';

for (const server of config.servers) {
  const { rows } = await poolFor(server.key).query<{ plain_text: string; n: number }>(
    `SELECT plain_text, count(*)::int AS n
       FROM chat_messages
      WHERE kind = 'death'
        AND (plain_text ILIKE '%teleport%' OR plain_text ILIKE '%tpa%' OR plain_text ILIKE '%request%')
      GROUP BY plain_text ORDER BY n DESC LIMIT 20`,
  );
  if (rows.length) {
    console.log(`\n${server.key}:`);
    for (const r of rows) console.log(`  ${r.n}x  ${r.plain_text.slice(0, 110)}`);
  }
}
await closeAllPools();
