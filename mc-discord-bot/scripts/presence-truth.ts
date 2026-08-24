/**
 * Compares both presence views against the server's own player count, asked for
 * over the vanilla status ping. The ping is the only outside opinion available,
 * so it is what tells us whether a view is inventing players.
 *
 * Run with `npx tsx scripts/presence-truth.ts`. Read-only.
 */
import net from 'node:net';

import { config } from '../src/config.js';
import { closeAllPools, poolFor } from '../src/db.js';

function varInt(value: number): Buffer {
  const out: number[] = [];
  let rest = value | 0;
  for (;;) {
    const byte = rest & 0x7f;
    // Unsigned shift, so a negative protocol version terminates after 5 bytes
    // instead of spinning forever on sign extension.
    rest >>>= 7;
    if (rest === 0) {
      out.push(byte);
      break;
    }
    out.push(byte | 0x80);
  }
  return Buffer.from(out);
}

function str(value: string): Buffer {
  const body = Buffer.from(value, 'utf8');
  return Buffer.concat([varInt(body.length), body]);
}

function packet(...parts: Buffer[]): Buffer {
  const body = Buffer.concat(parts);
  return Buffer.concat([varInt(body.length), body]);
}

function readVarInt(buffer: Buffer, offset: number): { value: number; size: number } | undefined {
  let value = 0;
  let size = 0;
  for (;;) {
    if (offset + size >= buffer.length) return undefined;
    const byte = buffer[offset + size]!;
    value |= (byte & 0x7f) << (7 * size);
    size += 1;
    if ((byte & 0x80) === 0) return { value, size };
    if (size > 5) throw new Error('varint too long');
  }
}

interface Status {
  online: number;
  max: number;
}

/** Vanilla server list ping: handshake into state 1, then ask for status. */
function ping(host: string, port: number, timeoutMs = 6000): Promise<Status> {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host, port });
    let chunks = Buffer.alloc(0);
    let settled = false;

    const finish = (error?: Error, status?: Status) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      if (error) reject(error);
      else resolve(status!);
    };

    socket.setTimeout(timeoutMs, () => finish(new Error('timed out')));
    socket.on('error', (error) => finish(error));

    socket.on('connect', () => {
      // -1 as the protocol version: every server answers a status request
      // regardless, and this avoids pretending to be a specific release.
      socket.write(packet(varInt(0x00), varInt(-1), str(host), Buffer.from([port >> 8, port & 0xff]), varInt(1)));
      socket.write(packet(varInt(0x00)));
    });

    socket.on('data', (chunk) => {
      chunks = Buffer.concat([chunks, chunk]);

      const length = readVarInt(chunks, 0);
      if (!length) return;
      if (chunks.length < length.size + length.value) return;

      try {
        let at = length.size;
        const id = readVarInt(chunks, at)!;
        at += id.size;
        const json = readVarInt(chunks, at)!;
        at += json.size;

        const payload = JSON.parse(chunks.subarray(at, at + json.value).toString('utf8'));
        finish(undefined, { online: payload?.players?.online ?? -1, max: payload?.players?.max ?? -1 });
      } catch (error) {
        finish(error as Error);
      }
    });
  });
}

interface Counts {
  logger: number;
  bot: number;
  host: string;
  port: number;
}

async function counts(key: string): Promise<Counts> {
  const pool = poolFor(key);
  const address = await pool.query<{ server_host: string; server_port: number }>(
    'SELECT server_host, server_port FROM sessions ORDER BY started_at DESC LIMIT 1',
  );
  const logger = await pool.query<{ count: number }>('SELECT count(*) AS count FROM online_now');
  const bot = await pool.query<{ count: number }>('SELECT count(*) AS count FROM discord_online_now');

  return {
    logger: logger.rows[0]!.count,
    bot: bot.rows[0]!.count,
    host: address.rows[0]?.server_host ?? '',
    port: address.rows[0]?.server_port ?? 25565,
  };
}

function off(view: number, truth: number): string {
  if (truth < 0) return '';
  const delta = view - truth;
  if (delta === 0) return 'exact';
  return `${delta > 0 ? '+' : ''}${delta} (${Math.round((Math.abs(delta) / Math.max(truth, 1)) * 100)}% off)`;
}

console.log('server        actual   logger_view          bot_view');

for (const server of config.servers) {
  try {
    const { logger, bot, host, port } = await counts(server.key);
    let truth = -1;
    let note = '';

    try {
      truth = (await ping(host, port)).online;
    } catch (error) {
      note = ` ping failed: ${(error as Error).message}`;
    }

    console.log(
      `${server.key.padEnd(12)} ${String(truth === -1 ? '?' : truth).padStart(6)}   ` +
        `${String(logger).padStart(5)} ${off(logger, truth).padEnd(14)} ` +
        `${String(bot).padStart(5)} ${off(bot, truth)}${note}`,
    );
  } catch (error) {
    console.log(`${server.key.padEnd(12)} unreachable: ${(error as Error).message}`);
  }
}

await closeAllPools();
