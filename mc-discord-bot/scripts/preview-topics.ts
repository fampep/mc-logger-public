/**
 * Prints the channel topic each server would get right now, with its length and
 * the channel it would be written to. Nothing is sent to Discord.
 *
 * Run with `npx tsx scripts/preview-topics.ts`, or with `--live` to also read
 * back what each channel is currently showing.
 */
import { REST, Routes } from 'discord.js';

import { config } from '../src/config.js';
import { closeAllPools, ensureBridgeState, getBridgeSettings, topicStats } from '../src/db.js';
import { renderTopic } from '../src/topic.js';
import { LIMITS } from '../src/ui.js';

const live = process.argv.includes('--live');
const rest = live ? new REST().setToken(config.discord.token) : null;

async function currentTopic(channelId: string): Promise<string> {
  if (!rest) return '';
  try {
    const channel = (await rest.get(Routes.channel(channelId))) as { name?: string; topic?: string | null };
    return `#${channel.name ?? '?'}: ${channel.topic || '(empty)'}`;
  } catch (error) {
    return `could not read channel: ${(error as Error).message}`;
  }
}

for (const server of config.servers) {
  try {
    await ensureBridgeState(server.key);
    const settings = await getBridgeSettings(server.key);
    const topic = renderTopic(server, await topicStats(server.key));

    console.log(
      `\n${server.key} → channel ${settings.channelId ?? 'not set'} ` +
        `(${topic.length}/${LIMITS.topic} chars${settings.enabled ? '' : ', feed paused'})`,
    );
    console.log(`  would set: ${topic}`);
    if (live && settings.channelId) console.log(`  now shows: ${await currentTopic(settings.channelId)}`);
  } catch (error) {
    console.log(`\n${server.key} failed: ${(error as Error).message}`);
  }
}

await closeAllPools();
