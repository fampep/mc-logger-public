#!/usr/bin/env tsx
/**
 * Probe every configured logger server for SLP version + GameSpy query plugins.
 *
 *   npm run probe-plugins
 *   npm run probe-plugins -- sixbsixt oldfrog
 *
 * Reads MC_HOST from azalea-bot/.env.* when Discord bot env has no SERVER_*_HOST.
 */
import { readFileSync, readdirSync, existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { config } from '../src/config.js';
import { DEFAULT_MC_HOSTS, probeServer, type ServerProbeResult } from '../src/serverProbe.js';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(here, '..', '..');
const azaleaDir = join(repoRoot, 'azalea-bot');

function hostFromAzaleaEnv(serverKey: string): { host?: string; port: number } {
  const candidates = [
    join(azaleaDir, `.env.${serverKey}`),
    join(azaleaDir, `.env.${serverKey.replace(/sixbsixt/, '6b6t').replace(/twobtwot/, '2b2t').replace(/ninebninet/, '9b9t')}`),
  ];

  for (const path of candidates) {
    if (!existsSync(path)) continue;
    const text = readFileSync(path, 'utf8');
    const host = text.match(/^MC_HOST=(.+)$/m)?.[1]?.trim();
    const port = Number(text.match(/^MC_PORT=(.+)$/m)?.[1]?.trim() ?? 25565);
    if (host) return { host, port: Number.isFinite(port) ? port : 25565 };
  }
  return { port: 25565 };
}

function resolveTarget(serverKey: string): { label: string; host: string; port: number } | null {
  const configured = config.servers.find((s) => s.key === serverKey);
  if (configured?.mcHost) {
    return { label: configured.label, host: configured.mcHost, port: configured.mcPort };
  }

  const fromEnv = hostFromAzaleaEnv(serverKey);
  const defaults = DEFAULT_MC_HOSTS[serverKey];
  const host = fromEnv.host ?? defaults?.host;
  if (!host) return null;

  return {
    label: configured?.label ?? serverKey,
    host,
    port: fromEnv.port ?? defaults?.port ?? 25565,
  };
}

function printResult(label: string, result: ServerProbeResult): void {
  console.log(`\n=== ${label} (${result.host}:${result.port}) ===`);
  if (!result.reachable) {
    console.log('  unreachable:', result.error ?? 'unknown');
    return;
  }
  console.log('  version:', result.version ?? '—');
  if (result.playersOnline != null) {
    console.log('  players:', `${result.playersOnline}/${result.playersMax ?? '?'}`);
  }
  if (result.motd) console.log('  motd:', result.motd.slice(0, 120));
  console.log('  latency:', `${result.latencyMs ?? '?'} ms`);
  if (result.queryEnabled && result.plugins.length > 0) {
    console.log('  software:', result.software ?? result.pluginsRaw?.split(';')[0]?.trim() ?? '—');
    console.log('  plugins:', result.plugins.join(', '));
  } else {
    console.log('  plugins: (query disabled or hidden — only ping version available)');
  }
}

async function main(): Promise<void> {
  const args = process.argv.slice(2).map((a) => a.toLowerCase());
  const keys =
    args.length > 0
      ? args
      : config.servers.length > 0
        ? config.servers.map((s) => s.key)
        : readdirSync(azaleaDir)
            .filter((name) => name.startsWith('.env.') && !name.endsWith('.example') && !name.endsWith('.disabled'))
            .map((name) => name.slice('.env.'.length));

  if (keys.length === 0) {
    console.error('No servers to probe. Set SERVERS in mc-discord-bot/.env or pass keys on the command line.');
    process.exit(1);
  }

  for (const key of keys) {
    const target = resolveTarget(key);
    if (!target) {
      console.log(`\n=== ${key} ===\n  skip: no host (set SERVER_*_HOST or azalea-bot/.env.${key})`);
      continue;
    }
    const result = await probeServer(target.host, target.port);
    printResult(target.label, result);
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
