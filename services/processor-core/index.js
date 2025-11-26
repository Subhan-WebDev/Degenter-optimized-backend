// services/processor-core/index.js
import '../../common/load-env.js';
process.env.SVC_NAME = process.env.SVC_NAME || 'processor-core';

import { createRedisClient } from '../../common/redis-client.js';
import { createConsumerGroup, readLoop } from '../../common/streams.js';
import { info, warn, err } from '../../common/log.js';
import { drainTrades } from '../../common/core/trades.js';
import { drainOHLCV } from '../../common/core/ohlcv.js';
import { parseBlock } from './parser/parse.js';

const STREAM_IN   = process.env.STREAM_RAW || 'chain:raw_blocks';
const GROUP       = process.env.STREAM_GROUP || 'processor';
const CONSUMER    = process.env.CONSUMER || `proc-${Math.random().toString(36).slice(2,8)}`;

const STREAM_OUTS = {
  new_pool:  process.env.STREAM_NEW_POOL  || 'events:new_pool',
  swap:      process.env.STREAM_SWAP      || 'events:swap',
  liquidity: process.env.STREAM_LIQUIDITY || 'events:liquidity',
  price:     process.env.STREAM_PRICE     || 'events:price_tick'
};

function onExit() {
  Promise.allSettled([drainTrades(), drainOHLCV()])
    .finally(() => process.exit(0));
}
['SIGINT','SIGTERM','beforeExit'].forEach(sig => process.on(sig, onExit));

// ✅ unified, ordered stream
const EVENTS_STREAM = process.env.EVENTS_STREAM || 'events:core';

const PRIORITY = { new_pool: 0, liquidity: 1, swap: 2, price: 3 };

const { client: redis, connect: redisConnect } = createRedisClient('processor');

async function emit(stream, obj) {
  return redis.xAdd(stream, '*', { j: JSON.stringify(obj) });
}

function toTsMs(x) {
  if (!x) return Date.now();
  const n = Date.parse(x);
  return Number.isFinite(n) ? n : Date.now();
}

async function emitCore(kind, meta, payload) {
  // meta should include tx_hash, height, created_at (or we’ll best-effort fill)
  const tx        = meta?.tx_hash || meta?.tx || null;
  const msgIndex  = meta?.msg_index ?? 0;
  const height    = meta?.height ?? 0;
  const ts        = meta?.created_at || meta?.at || null;

  // Flat fields for fast scanning; payload holds the original object
  return redis.xAdd(EVENTS_STREAM, '*', {
    kind,
    tx: tx || '',
    msg_index: String(msgIndex),
    height: String(height),
    ts: String(toTsMs(ts)),
    payload: JSON.stringify(payload),
  });
}

async function handler(records, { ackMany }) {
  const ids = [];
  for (const rec of records) {
    ids.push(rec.id);
    try {
      const raw = rec.map?.j ? JSON.parse(rec.map.j) : null;
      if (!raw) continue;

      const out = await parseBlock(raw);

      // ── keep existing per-type streams ────────────────────────────
      for (const p of out.pools)   await emit(STREAM_OUTS.new_pool,  p);
      for (const s of out.swaps)   await emit(STREAM_OUTS.swap,      s);
      for (const l of out.liqs)    await emit(STREAM_OUTS.liquidity, l);
      for (const t of out.prices)  await emit(STREAM_OUTS.price,     t);

      // ── ALSO publish to unified ordered stream (`events:core`) ────
      // Build tx-bundles then sort inside each tx by (priority, msg_index)
      const bundleByTx = new Map();

      const push = (kind, obj) => {
        const tx = obj?.tx_hash || '';
        const arr = bundleByTx.get(tx) || [];
        arr.push({ kind, obj });
        bundleByTx.set(tx, arr);
      };

      for (const p of out.pools)   push('new_pool',  p);
      for (const l of out.liqs)    push('liquidity', l);
      for (const s of out.swaps)   push('swap',      s);
      for (const t of out.prices)  push('price',     t);

      for (const [tx, list] of bundleByTx) {
        list.sort((a, b) => {
          const pa = PRIORITY[a.kind] ?? 999;
          const pb = PRIORITY[b.kind] ?? 999;
          if (pa !== pb) return pa - pb;
          const ia = Number(a.obj?.msg_index ?? 0);
          const ib = Number(b.obj?.msg_index ?? 0);
          return ia - ib;
        });
        for (const item of list) {
          await emitCore(item.kind, item.obj, item.obj);
        }
      }

    } catch (e) {
      warn('parse error', e?.message || e);
    }
  }
  await ackMany(ids);
}

async function main() {
  await redisConnect();
  await createConsumerGroup({ redis, stream: STREAM_IN, group: GROUP });
  info('processor ready, reading…');

  await readLoop({
    redis,
    stream: STREAM_IN,
    group: GROUP,
    consumer: CONSUMER,
    handler,
    batch: Number(process.env.PROCESSOR_BATCH || 50),
    blockMs: Number(process.env.PROCESSOR_BLOCK_MS || 5000)
  });
}

main().catch(e => { err(e); process.exit(1); });
