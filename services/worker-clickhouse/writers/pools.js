
// services/worker-clickhouse/writers/pools.js
import { chInsertJSON, chQuery, toChDateTime } from '../../../common/db-clickhouse.js';
import BatchQueue from '../../../common/batch.js';
import { info, warn } from '../../../common/log.js';
import { getPoolMeta } from '../pool_resolver.js';

const tokenSeen = new Set();
const tokenBuffer = [];
const poolSeen = new Set(); // process-level dedupe so we don't insert the same pool twice

const poolsQueue = new BatchQueue({
  maxItems: Number(process.env.CLICKHOUSE_POOL_BUFFER || 200),
  maxWaitMs: Number(process.env.CLICKHOUSE_POOL_FLUSH_MS || 2000),
  flushFn: async (items) => {
    if (items.length) {
      await chInsertJSON({
        table: 'pools',
        rows: items,
        settings: { deduplicate_by_primary_key: 1 }
      });
    }
    if (tokenBuffer.length) {
      const tokens = tokenBuffer.splice(0, tokenBuffer.length);
      await chInsertJSON({
        table: 'tokens',
        rows: tokens,
        settings: { deduplicate_by_primary_key: 1 }
      });
    }
  }
});

async function poolExists(pool_id) {
  const num = Number(pool_id);
  if (!Number.isFinite(num)) return false;
  if (poolSeen.has(num)) return true;
  try {
    const rows = await chQuery({
      query: 'SELECT 1 FROM pools WHERE pool_id = {pool_id:UInt64} LIMIT 1',
      params: { pool_id: num }
    });
    if (rows?.length) {
      poolSeen.add(num);
      return true;
    }
  } catch (err) {
    warn('[ch/pool/exists]', { pool_id: pool_id, err: err?.message || err });
  }
  return false;
}

function pushToken(meta, created_at) {
  if (!meta?.token_id || tokenSeen.has(meta.token_id)) return;
  tokenSeen.add(meta.token_id);
  tokenBuffer.push({
    token_id: meta.token_id,
    denom: meta.denom || '',
    type: '',
    name: meta.denom || '',
    symbol: '',
    display: meta.denom || '',
    exponent: Number(meta.exponent ?? 6),
    image_uri: '',
    website: '',
    twitter: '',
    telegram: '',
    max_supply_base: '0',
    total_supply_base: '0',
    description: '',
    created_at: toChDateTime(created_at)
  });
}

export async function flushPools() {
  await poolsQueue.drain();
}

export async function handlePoolEvent(e) {
  try {
    if (!e?.pair_contract) { warn('[ch/pool] missing pair_contract on event'); return true; }

    const meta = await getPoolMeta(e.pair_contract);
    if (!meta) { warn('[ch/pool] missing pool meta', e.pair_contract); return false; }
    if (!meta.pool_id && !e.pool_id) { warn('[ch/pool] missing pool_id', { pair_contract: e.pair_contract, event_pool_id: e.pool_id }); return false; }
    const poolId = meta.pool_id || e.pool_id;

    // Skip if we already have this pool recorded (ClickHouse or this process)
    if (await poolExists(poolId)) {
      info('[ch/pool] already exists, skip', { pool_id: poolId, pair_contract: e.pair_contract });
      return true;
    }

    pushToken({ token_id: meta.base_token_id, denom: meta.base_denom, exponent: meta.base_exp }, e.created_at);
    pushToken({ token_id: meta.quote_token_id, denom: meta.quote_denom, exponent: meta.quote_exp }, e.created_at);

    poolsQueue.push({
      pool_id: poolId,
      pair_contract: e.pair_contract,
      base_token_id: meta.base_token_id,
      quote_token_id: meta.quote_token_id,
      lp_token_denom: '',
      pair_type: e.pair_type || 'xyk',
      is_uzig_quote: meta.is_uzig_quote ? 1 : 0,
      factory_contract: process.env.FACTORY_ADDR || '',
      router_contract: process.env.ROUTER_ADDR || '',
      created_at: toChDateTime(e.created_at),
      created_height: Number(e.height || 0),
      created_tx_hash: e.tx_hash || '',
      signer: e.signer || ''
    });

    poolSeen.add(poolId);

    info('[ch] pool recorded', e.pair_contract, meta.pool_id);
    return true;
  } catch (err) {
    warn('[ch/pool]', err?.message || err);
    return false;
  }
}
