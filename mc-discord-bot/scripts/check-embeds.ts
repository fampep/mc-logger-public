/**
 * Builds every embed with hostile data — 400-character messages, names full of
 * markdown, 300 players online, a database that is down — and checks the result
 * against Discord's limits.
 *
 * Worth having because the failure mode is invisible locally: an embed over the
 * limit is rejected by the API at runtime, and an unbalanced code fence silently
 * swallows the rest of the message. discord.js validates most field lengths when
 * they are set, so overruns throw here instead of in production.
 *
 * Run with `npm run check`. No database or token needed.
 */
process.env.DATABASE_URL ??= 'postgres://user:pass@localhost:5432/db';
process.env.DISCORD_TOKEN ??= 'test';
process.env.DISCORD_CLIENT_ID ??= 'test';

const embeds = await import('../src/embeds.js');
const { dateTime, listText } = await import('../src/ui.js');
const { EmbedBuilder } = await import('discord.js');

type Embed = InstanceType<typeof EmbedBuilder>;

/** Rows on one screen of a paged list, as `/player` and `/stats` slice them. */
const ROWS_PER_SCREEN = 50;

/**
 * Lists that page must not drop a row: the footer promises "1-50 of 100" or
 * "page 2 of 9", so every row of that page has to actually be on it. Label to
 * the number of rows expected, since the feed and the online list are allowed
 * to truncate.
 */
const MUST_NOT_DROP = new Map<string, number>();

const LONG = 'x'.repeat(400);
const NASTY = '**_~~`|@everyone <@123> https://example.com/really/long/path`~~_**';
const NAMES = ['_Notch_', 'Herobrine', 'a**b', 'VeryLongPlayerName1', 'x', ...Array.from({ length: 300 }, (_, i) => `player_${i}`)];
const CONNECTED = { open: 1, latest_start: new Date(), last_ended_at: null, last_end_reason: null };

function chatRow(index: number, kind = 'chat') {
  return {
    id: index,
    received_at: new Date(Date.now() - index * 1000),
    kind,
    sender_name: kind === 'chat' ? NAMES[index % NAMES.length]! : null,
    subject_name: kind === 'chat' ? null : NAMES[index % NAMES.length]!,
    killer_name: kind === 'death' ? 'Herobrine' : null,
    content: kind === 'chat' ? `${NASTY} ${LONG}` : null,
    plain_text: `${NAMES[index % NAMES.length]} did something ${NASTY} ${LONG}`,
    server_host: 'constantiam.net',
  };
}

const leaders = Array.from({ length: 25 }, (_, i) => ({
  name: NAMES[i % NAMES.length]!,
  value: 999_999 - i * 1000,
  kills: 999_999 - i * 1000,
  deaths: i * 7,
}));

MUST_NOT_DROP.set('player list', ROWS_PER_SCREEN);
MUST_NOT_DROP.set('chat page (15 per page)', 15);

const cases: Array<[string, () => Embed]> = [
  ['event embed (chat)', () => embeds.buildEventEmbed(chatRow(1), 'Constantiam')],
  ['event embed (death)', () => embeds.buildEventEmbed(chatRow(2, 'death'), 'Constantiam')],
  [
    'feed batch (60 long lines)',
    () => embeds.buildFeedBatchEmbed(Array.from({ length: 60 }, (_, i) => chatRow(i)), 'Constantiam'),
  ],
  [
    'stats overview',
    () =>
      embeds.buildStatsOverviewEmbed({
        serverLabel: 'Constantiam',
        totals: {
          servers: 3,
          sessions: 12_345,
          messages: 9_876_543,
          chat: 1_234_567,
          players: 54_321,
          deaths: 8_765,
          kills: 432,
          online: 87,
          first_seen: new Date('2024-01-01'),
          last_seen: new Date(),
        },
        breakdown: Array.from({ length: 10 }, (_, i) => ({
          server_host: `some-really-long-hostname-${i}.example.net`,
          sessions: 1234,
          avg_secs: 4321.5,
          max_secs: 98_765,
          messages: 1_234_567,
        })),
        topChatters: leaders.slice(0, 8),
        topKillers: leaders.slice(0, 5),
      }),
  ],
  ...(['chat', 'join', 'leave', 'death', 'kill', 'advancement'] as const).map((kind) => {
    const label = `events page (${kind})`;
    MUST_NOT_DROP.set(label, ROWS_PER_SCREEN);
    return [
      label,
      () =>
        embeds.buildEventsPageEmbed({
          kind,
          serverLabel: 'Constantiam',
          note: `1-${ROWS_PER_SCREEN} of 100`,
          rows: Array.from({ length: ROWS_PER_SCREEN }, (_, i) => ({
            occurred_at: new Date(),
            player_name: NAMES[i % NAMES.length]!,
            detail: `${NASTY} ${LONG}`,
          })),
        }),
    ] as [string, () => Embed];
  }),
  [
    'player overview',
    () =>
      embeds.buildPlayerOverviewEmbed({
        stats: {
          name: '_Notch_',
          messages: 12_345,
          deaths: 678,
          joins: 910,
          leaves: 900,
          kills: 1112,
          advancements: 13,
          first_seen: new Date('2024-02-03'),
          last_seen: new Date(),
          last_message_at: new Date(),
          kill_rank: 4,
        },
        serverLabel: 'Constantiam',
        aliases: Array.from({ length: 12 }, (_, i) => ({ name: `oldname_${i}`, seen_at: new Date() })),
        rivals: {
          victims: leaders.slice(0, 5).map((row) => ({ name: row.name, count: row.kills })),
          nemeses: leaders.slice(0, 5).map((row) => ({ name: row.name, count: row.deaths })),
        },
        servers: ['constantiam.net', 'purityvanilla.com', '2b2t.org'],
        onlineOn: 'constantiam.net',
      }),
  ],
  [
    'player list',
    () =>
      embeds.buildPlayerListEmbed({
        name: '_Notch_',
        serverLabel: 'Constantiam',
        title: 'Recent messages',
        color: 0x5865f2,
        // Exactly as the command builds them: a full date, then a bounded line.
        lines: Array.from({ length: ROWS_PER_SCREEN }, () => `${dateTime(new Date())}  ${listText(`${NASTY} ${LONG}`)}`),
        empty: 'nothing',
        note: `1-${ROWS_PER_SCREEN} of 100`,
      }),
  ],
  [
    'online (300 players, 3 servers)',
    () =>
      embeds.buildOnlineEmbed({
        serverLabel: 'Constantiam',
        total: 300,
        connection: CONNECTED,
        rows: Array.from({ length: 300 }, (_, i) => ({
          server_host: `host-${i % 3}.example.net`,
          player_name: NAMES[i % NAMES.length]!,
          occurred_at: new Date(),
        })),
      }),
  ],
  [
    'online (logger disconnected, long reason)',
    () =>
      embeds.buildOnlineEmbed({
        serverLabel: 'Constantiam',
        total: 0,
        rows: [],
        connection: { open: 0, latest_start: new Date(), last_ended_at: new Date(), last_end_reason: LONG },
      }),
  ],
  [
    // Chat defaults to 15; long messages would not all fit at 50.
    'chat page (15 per page)',
    () =>
      embeds.buildChatEmbed({
        serverLabel: 'Constantiam',
        rows: Array.from({ length: 15 }, (_, i) => ({
          received_at: new Date(),
          sender_name: NAMES[i % NAMES.length]!,
          plain_text: LONG,
          content: `${NASTY} ${LONG}`,
          server_host: 'constantiam.net',
        })),
        search: NASTY,
        page: 3,
        pageCount: 25,
        total: 123_456,
      }),
  ],
  [
    'top boards',
    () =>
      embeds.buildTopBoardsEmbed({
        serverLabel: 'Constantiam',
        topChatters: leaders.slice(0, 8),
        topKillers: leaders.slice(0, 8),
        topDeaths: leaders.slice(0, 8),
        topKd: leaders.slice(0, 8),
      }),
  ],
  ...(['kills', 'kd', 'deaths', 'messages', 'joins'] as const).map(
    (metric) =>
      [
        `leaderboard (${metric})`,
        () =>
          embeds.buildLeaderboardEmbed({
            metric,
            rows: leaders,
            serverLabel: 'Constantiam',
            page: 0,
            pageCount: 20,
            total: 500,
            perPage: 25,
          }),
      ] as [string, () => Embed],
  ),
  [
    'activity (365 days)',
    () =>
      embeds.buildActivityEmbed({
        serverLabel: 'Constantiam',
        days: 365,
        hourly: Array.from({ length: 24 }, (_, h) => ({ hour: h, messages: h * 54_321 })),
        daily: Array.from({ length: 365 }, (_, d) => ({
          day: `2025-${String((d % 12) + 1).padStart(2, '0')}-15`,
          messages: d * 1000,
        })),
        topPlayers: leaders.slice(0, 5),
      }),
  ],
  [
    'sessions',
    () =>
      embeds.buildSessionsEmbed({
        serverLabel: 'Constantiam',
        page: 0,
        pageCount: 25,
        summary: {
          sessions: 4321,
          active: 1,
          avg_secs: 1234.5,
          max_secs: 98_765,
          total_secs: 12_345_678,
          latest_start: new Date(),
        },
        reasons: Array.from({ length: 6 }, (_, i) => ({ name: `a very long end reason number ${i}`, value: 100 - i })),
        rows: Array.from({ length: 10 }, (_, i) => ({
          id: i,
          server_host: 'constantiam.net',
          target_version: '1.21.10',
          client: 'azalea',
          bot_username: 'logger',
          started_at: new Date(),
          ended_at: i === 0 ? null : new Date(),
          end_reason: 'connection reset by peer, which is quite long',
          secs: 12_345,
          messages: 6789,
        })),
      }),
  ],
  [
    'database',
    () =>
      embeds.buildDatabaseEmbed(
        {
          database: 'constantiam',
          size: '12 GB',
          bytes: 12e9,
          tables: Array.from({ length: 12 }, (_, i) => ({
            table_name: `some_long_table_name_${i}`,
            rows: 12_345_678,
            total_size: '1234 MB',
            table_size: '1000 MB',
            index_size: '234 MB',
            bytes: 1e9,
          })),
          oldest: new Date('2024-01-01'),
          newest: new Date(),
          counts: {
            chat: 1e6,
            joins: 2e5,
            leaves: 2e5,
            deaths: 3e4,
            kills: 4e3,
            players: 5e4,
            sessions: 6e3,
            names: 7e4,
          },
        },
        'Constantiam',
      ),
  ],
  [
    'status (5 servers, one down)',
    () =>
      embeds.buildStatusEmbed({
        uptimeSecs: 987_654,
        gatewayMs: 42,
        servers: Array.from({ length: 5 }, (_, i) => ({
          label: `Server ${i}`,
          reachable: i !== 2,
          error: i === 2 ? LONG : undefined,
          latencyMs: 4,
          online: 87,
          lastMessageAt: new Date(),
          behind: i,
          bridgeEnabled: i % 2 === 0,
          channelId: i === 3 ? null : '123456789',
          watches: 12,
          logger:
            i === 1
              ? { open: 0, latest_start: new Date(), last_ended_at: new Date(), last_end_reason: LONG }
              : CONNECTED,
        })),
      }),
  ],
  [
    'watch list',
    () =>
      embeds.buildWatchListEmbed({
        serverLabel: 'Constantiam',
        scope: 'everyone',
        rows: Array.from({ length: 25 }, (_, i) => ({
          player_name: NAMES[i % NAMES.length]!,
          user_id: '123456789012345678',
          channel_id: '987654321098765432',
          events: (['join', 'leave', 'both'] as const)[i % 3]!,
          created_at: new Date(),
        })),
      }),
  ],
  [
    'bridge status',
    () =>
      embeds.buildBridgeStatusEmbed(
        Array.from({ length: 5 }, (_, i) => ({
          label: `Server ${i}`,
          channelId: i === 1 ? null : '123',
          enabled: i % 2 === 0,
          error: i === 4 ? LONG : undefined,
          behind: i * 100,
        })),
      ),
  ],
  [
    'help',
    () =>
      embeds.buildHelpEmbed([
        { group: 'Group one', entries: [['/a', 'does a thing'], ['/b', 'does another']] },
        { group: 'Group two', entries: [['/c', 'more']] },
      ]),
  ],
];

/** Discord counts title, description, fields, footer, and author together. */
function totalLength(embed: Embed): number {
  const json = embed.toJSON();
  return (
    (json.title?.length ?? 0) +
    (json.description?.length ?? 0) +
    (json.footer?.text?.length ?? 0) +
    (json.author?.name?.length ?? 0) +
    (json.fields ?? []).reduce((sum, field) => sum + field.name.length + field.value.length, 0)
  );
}

let failures = 0;

for (const [label, build] of cases) {
  try {
    const embed = build();
    const json = embed.toJSON();
    const total = totalLength(embed);
    const problems: string[] = [];

    if (total > 6000) problems.push(`total ${total} > 6000`);
    if ((json.description?.length ?? 0) > 4096) problems.push(`description ${json.description!.length}`);
    if ((json.fields?.length ?? 0) > 25) problems.push(`${json.fields!.length} fields`);
    for (const field of json.fields ?? []) {
      if (field.value.length > 1024) problems.push(`field "${field.name}" value ${field.value.length}`);
      if (field.name.length > 256) problems.push(`field name ${field.name.length}`);
      // An unbalanced code fence swallows the rest of the message.
      const fences = (field.value.match(/```/g) ?? []).length;
      if (fences % 2 !== 0) problems.push(`field "${field.name}" has ${fences} code fences`);
    }
    const bodyFences = (json.description?.match(/```/g) ?? []).length;
    if (bodyFences % 2 !== 0) problems.push(`description has ${bodyFences} code fences`);

    const expected = MUST_NOT_DROP.get(label);
    if (expected !== undefined) {
      const rendered = json.description?.split('\n').length ?? 0;
      if (json.description?.includes('…and ')) problems.push('dropped rows the footer counts');
      if (rendered < expected) problems.push(`only ${rendered} of ${expected} rows rendered`);
    }

    if (problems.length > 0) {
      failures++;
      console.log(`FAIL  ${label}: ${problems.join('; ')}`);
    } else {
      console.log(`ok    ${label} (total ${total}, ${json.fields?.length ?? 0} fields)`);
    }
  } catch (error) {
    failures++;
    console.log(`THROW ${label}: ${(error as Error).message.split('\n')[0]}`);
  }
}

// The live feed must never lose a line, no matter how long the messages are.
for (const maxLines of [1, 5, 20, 60]) {
  const rows = Array.from({ length: 200 }, (_, i) => chatRow(i, i % 4 === 0 ? 'death' : 'chat'));
  const groups = embeds.packFeed(rows, maxLines);
  const packed = groups.reduce((sum, group) => sum + group.length, 0);
  const problems: string[] = [];

  if (packed !== rows.length) problems.push(`packed ${packed} of ${rows.length} rows`);
  for (const group of groups) {
    if (group.length > maxLines) problems.push(`group of ${group.length} exceeds ${maxLines}`);
    const description = embeds.buildFeedBatchEmbed(group, 'Constantiam').toJSON().description ?? '';
    if (description.includes('…and ')) problems.push(`a group was truncated at ${group.length} lines`);
    if (description.split('\n').length !== group.length) {
      problems.push(`rendered ${description.split('\n').length} lines for ${group.length} rows`);
    }
  }

  if (problems.length > 0) {
    failures++;
    console.log(`FAIL  feed packing (max ${maxLines}): ${[...new Set(problems)].join('; ')}`);
  } else {
    console.log(`ok    feed packing (max ${maxLines}): ${rows.length} rows in ${groups.length} messages, none lost`);
  }
}

console.log(failures === 0 ? '\nAll embeds within limits.' : `\n${failures} problem(s).`);

// Eyeball one of each layout style.
const show = (label: string, embed: Embed) => {
  const json = embed.toJSON();
  console.log(`\n=== ${label} ===`);
  if (json.title) console.log(`# ${json.title}`);
  if (json.description) console.log(json.description);
  for (const field of json.fields ?? []) console.log(`\n[${field.name}]\n${field.value}`);
  if (json.footer) console.log(`\n-- ${json.footer.text}`);
};

show(
  'leaderboard',
  embeds.buildLeaderboardEmbed({
    metric: 'kills',
    rows: leaders.slice(0, 10),
    serverLabel: 'Constantiam',
    page: 0,
    pageCount: 4,
    total: 37,
    perPage: 10,
  }),
);

show(
  'activity',
  embeds.buildActivityEmbed({
    serverLabel: 'Constantiam',
    days: 14,
    hourly: Array.from({ length: 24 }, (_, h) => ({ hour: h, messages: Math.round(Math.sin(h / 3) * 400 + 420) })),
    daily: Array.from({ length: 14 }, (_, d) => ({ day: `2026-08-${String(d + 1).padStart(2, '0')}`, messages: d * 137 })),
    topPlayers: leaders.slice(0, 5),
  }),
);

show(
  'online',
  embeds.buildOnlineEmbed({
    serverLabel: 'Constantiam',
    total: 7,
    connection: CONNECTED,
    rows: ['Notch', 'Herobrine', 'Dinnerbone', 'jeb_', 'Grumm', 'a_very_long_name1', 'x'].map((name) => ({
      server_host: 'constantiam.net',
      player_name: name,
      occurred_at: new Date(),
    })),
  }),
);

show('feed batch', embeds.buildFeedBatchEmbed([chatRow(0), chatRow(1, 'join'), chatRow(2, 'death')], 'Constantiam'));

process.exit(failures === 0 ? 0 : 1);
