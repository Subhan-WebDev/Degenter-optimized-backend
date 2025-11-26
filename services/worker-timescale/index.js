// services/worker-timescale/index.js
import '../../common/load-env.js';
process.env.SVC_NAME = process.env.SVC_NAME || 'worker-timescale';

import { createRedisClient } from '../../common/redis-client.js';
import { createConsumerGroup, readLoop } from '../../common/streams.js';
import { info, warn, err } from '../../common/log.js';

import { preloadPools } from '../../common/core/pools_cache.js';
import { handlePoolEvent } from './writers/pools.js';
import { handleSwapEvent, handleLiquidityEvent } from './writers/trades.js';
import { handlePriceSnapshot } from './writers/prices.js';

// drain on shutdown (batch paths)
import { drainTrades } from '../../common/core/trades.js';
import { drainOHLCV } from '../../common/core/ohlcv.js';

/* ───────────────────────── Streams ───────────────────────── */
const STREAMS = {
  new_pool:  process.env.STREAM_NEW_POOL  || 'events:new_pool',
  swap:      process.env.STREAM_SWAP      || 'events:swap',
  liquidity: process.env.STREAM_LIQUIDITY || 'events:liquidity',
  price:     process.env.STREAM_PRICE     || 'events:price_tick'
};

// Unified, ordered stream from processor-core
const EVENTS_STREAM = process.env.EVENTS_STREAM || 'events:core';

/* ───────────────────────── Groups & perf ───────────────────────── */
const GROUP          = process.env.TIMESCALE_GROUP || 'timescale';
const BATCH          = Number(process.env.TIMESCALE_BATCH || 512);
const BLOCK_MS       = Number(process.env.TIMESCALE_BLOCK_MS || 1500);

// NEW: distinct reader counts for core vs legacy streams
const CORE_READERS   = Number(process.env.CORE_READERS || 1);           // keep at 1 to preserve order
const LEGACY_READERS = Number(process.env.TIMESCALE_READERS || 0);      // optional for legacy split streams

/* ───────────────────────── Priority for events:core ───────────────────────── */
const PRIORITY = { new_pool: 0, liquidity: 1, swap: 2, price: 3 };

function cmpCore(a, b) {
  // Sort by height asc, then priority asc, then msg_index asc, then redis id
  const ha = Number(a.height || 0), hb = Number(b.height || 0);
  if (ha !== hb) return ha - hb;

  const pa = PRIORITY[a.kind] ?? 999;
  const pb = PRIORITY[b.kind] ?? 999;
  if (pa !== pb) return pa - pb;

  const ia = Number(a.msg_index || 0), ib = Number(b.msg_index || 0);
  if (ia !== ib) return ia - ib;

  return (a.id > b.id) ? 1 : (a.id < b.id ? -1 : 0);
}

const { client: redis, connect: redisConnect } = createRedisClient('ts-writer');

/* ───────────────────────── Helpers ───────────────────────── */
async function makeReader(stream, handler, idx = 0) {
  await createConsumerGroup({ redis, stream, group: GROUP });
  const consumer = `${process.env.SVC_NAME}-${stream}-${idx}-${Math.random().toString(36).slice(2, 6)}`;
  info('timescale reader ready', { stream, group: GROUP, consumer, batch: BATCH, blockMs: BLOCK_MS });

  return readLoop({
    redis,
    stream,
    group: GROUP,
    consumer,
    batch: BATCH,
    blockMs: BLOCK_MS,
    handler: async (records, { ackMany }) => {
      const ids = [];
      for (const rec of records) {
        ids.push(rec.id);
        try {
          const obj = rec.map?.j ? JSON.parse(rec.map.j) : null;
          if (!obj) continue;
          await handler(obj);
        } catch (e) {
          warn(`[${stream}] handler`, e?.message || e);
        }
      }
      if (ids.length) await ackMany(ids);
    }
  });
}

async function makeCoreReader(idx) {
  const stream = EVENTS_STREAM;
  await createConsumerGroup({ redis, stream, group: GROUP });
  const consumer = `${process.env.SVC_NAME}-${stream}-${idx}-${Math.random().toString(36).slice(2, 6)}`;
  info('timescale CORE reader ready', { stream, group: GROUP, consumer, batch: BATCH, blockMs: BLOCK_MS, idx });

  return readLoop({
    redis,
    stream,
    group: GROUP,
    consumer,
    batch: BATCH,
    blockMs: BLOCK_MS,
    handler: async (records, { ackMany }) => {
      if (!records?.length) return;

      // Normalize and sort by (height, priority, msg_index) to enforce pool-first within batch
      const items = [];
      const ids = [];

      for (const rec of records) {
        ids.push(rec.id);
        const kind = rec.map?.kind || '';
        const payload = rec.map?.payload ? JSON.parse(rec.map.payload) : null;
        const height = Number(rec.map?.height || payload?.height || 0);
        const msg_index = Number(rec.map?.msg_index || payload?.msg_index || 0);

        if (!payload) continue;

        items.push({
          id: rec.id,
          kind,
          payload,
          height,
          msg_index
        });
      }

      items.sort(cmpCore);

      // Process in stable, sorted order
      for (const it of items) {
        try {
          switch (it.kind) {
            case 'new_pool':
              await handlePoolEvent(it.payload);
              break;
            case 'liquidity':
              await handleLiquidityEvent(it.payload);
              break;
            case 'swap':
              await handleSwapEvent(it.payload);
              break;
            case 'price':
              await handlePriceSnapshot(it.payload);
              break;
            default:
              break;
          }
        } catch (e) {
          warn('[events:core] route error', e?.message || e);
        }
      }

      if (ids.length) await ackMany(ids);
    }
  });
}

/* ───────────────────────── Shutdown ───────────────────────── */
async function gracefulExit(code = 0) {
  try {
    await drainTrades();
    await drainOHLCV();
  } catch (e) {
    err('[gracefulExit]', e?.message || e);
  } finally {
    try { await redis?.quit?.(); } catch {}
    process.exit(code);
  }
}
process.on('SIGINT',  () => gracefulExit(0));
process.on('SIGTERM', () => gracefulExit(0));
process.on('beforeExit', () => gracefulExit(0));

/* ───────────────────────── Main ───────────────────────── */
async function main() {
  info('worker-timescale config', {
    group: GROUP,
    batch: BATCH,
    blockMs: BLOCK_MS,
    core_stream: EVENTS_STREAM,
    CORE_READERS,
    LEGACY_READERS
  });

  await redisConnect();
  await preloadPools(); // warm the cache at boot

  // IMPORTANT: run a SINGLE reader for events:core to preserve order
  const coreReaders = [];
  const count = Math.max(1, CORE_READERS); // even if misconfigured, force >=1
  for (let i = 0; i < count; i++) coreReaders.push(makeCoreReader(i));
  await Promise.all(coreReaders);

  // (Optional) legacy readers in parallel — disabled by default
  if (process.env.ENABLE_LEGACY_STREAMS === '1' && LEGACY_READERS > 0) {
    const tasks = [];
    for (let i = 0; i < LEGACY_READERS; i++) {
      tasks.push(makeReader(STREAMS.new_pool,  handlePoolEvent,      i));
      tasks.push(makeReader(STREAMS.swap,      handleSwapEvent,      i));
      tasks.push(makeReader(STREAMS.liquidity, handleLiquidityEvent, i));
      tasks.push(makeReader(STREAMS.price,     handlePriceSnapshot,  i));
    }
    await Promise.all(tasks);
  }

  info('worker-timescale running');
}

main().catch(e => { err(e); process.exit(1); });
