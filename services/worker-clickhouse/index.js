
// services/worker-clickhouse/index.js
import '../../common/load-env.js';
process.env.SVC_NAME = process.env.SVC_NAME || 'worker-clickhouse';

import { createRedisClient } from '../../common/redis-client.js';
import { createConsumerGroup, readLoop } from '../../common/streams.js';
import { info, warn, err } from '../../common/log.js';
import { chPing, chInfo } from '../../common/db-clickhouse.js';

import { handlePoolEvent, flushPools } from './writers/pools.js';
import { handleSwapEvent, handleLiquidityEvent, flushTrades } from './writers/trades.js';
import { handlePriceSnapshot, flushPriceTicks } from './writers/prices.js';
import { flushPoolState } from './writers/pool_state.js';
import { initPoolResolver } from './pool_resolver.js';

const STREAMS = {
  new_pool:  process.env.STREAM_NEW_POOL  || 'events:new_pool',
  swap:      process.env.STREAM_SWAP      || 'events:swap',
  liquidity: process.env.STREAM_LIQUIDITY || 'events:liquidity',
  price:     process.env.STREAM_PRICE     || 'events:price_tick'
};

const GROUP     = process.env.CLICKHOUSE_GROUP || 'clickhouse';
const BATCH     = Number(process.env.CLICKHOUSE_BATCH || 256);
const BLOCK_MS  = Number(process.env.CLICKHOUSE_BLOCK_MS || 5000);

const { client: redis, connect: redisConnect } = createRedisClient('ch-writer');

async function makeReader(stream, handler) {
  await createConsumerGroup({ redis, stream, group: GROUP });
  const consumer = `${process.env.SVC_NAME}-${stream}-${Math.random().toString(36).slice(2, 8)}`;
  info('clickhouse reader ready', { stream, group: GROUP, consumer });

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
        try {
          const obj = rec.map?.j ? JSON.parse(rec.map.j) : null;
          if (!obj) { ids.push(rec.id); continue; }
          const ok = await handler(obj);
          if (ok !== false) ids.push(rec.id);
        } catch (e) {
          warn(`[${stream}] handler`, e?.message || e);
        }
      }
      await ackMany(ids);
    }
  });
}

async function main() {
  await redisConnect();
  info('clickhouse config', chInfo());
  try {
    await chPing();
  } catch (e) {
    err('[clickhouse] ping failed', e?.message || e);
    throw e;
  }
  initPoolResolver(redis);

  await Promise.all([
    makeReader(STREAMS.new_pool,  handlePoolEvent),
    makeReader(STREAMS.swap,      handleSwapEvent),
    makeReader(STREAMS.liquidity, handleLiquidityEvent),
    makeReader(STREAMS.price,     handlePriceSnapshot)
  ]);

  info('worker-clickhouse running');
}

process.on('SIGINT', async () => {
  await Promise.all([flushTrades(), flushPriceTicks(), flushPoolState(), flushPools()]);
  process.exit(0);
});

main().catch(e => { err(e); process.exit(1); });
