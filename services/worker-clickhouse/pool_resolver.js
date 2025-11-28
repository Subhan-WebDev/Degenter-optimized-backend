// services/worker-clickhouse/pool_resolver.js
import { poolWithTokensCached } from '../../common/core/pools_cache.js';
import { chQuery } from '../../common/db-clickhouse.js';

const CACHE_MAX = Number(process.env.POOL_CACHE_MAX || 2000);
const RETRY_MS = Number(process.env.POOL_ID_RETRY_MS || 500);
const MAX_RETRIES = Number(process.env.POOL_ID_MAX_RETRIES || 120); // wait up to ~1m by default
const KEY_PREFIX = process.env.REDIS_POOL_PREFIX || 'pool_id:';

let redis = null;
const CH_DB = process.env.CLICKHOUSE_DB || 'degenter';

const poolIdCache = new Map();       // pair_contract -> pool_id
const poolMetaCache = new Map();      // pair_contract -> metadata

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function remember(map, key, value) {
  if (map.has(key)) {
    map.delete(key);
  }
  map.set(key, value);
  if (map.size > CACHE_MAX) {
    const firstKey = map.keys().next().value;
    map.delete(firstKey);
  }
}

export function initPoolResolver(redisClient) {
  redis = redisClient;
}

async function fetchPoolMetaClickhouse(pair_contract) {
  if (!pair_contract) return null;
  const rows = await chQuery({
    query: `
      SELECT
        p.pool_id,
        p.pair_contract,
        p.is_uzig_quote,
        tb.token_id AS base_token_id, tb.denom AS base_denom, COALESCE(tb.exponent,6) AS base_exp,
        tq.token_id AS quote_token_id, tq.denom AS quote_denom, COALESCE(tq.exponent,6) AS quote_exp
      FROM ${CH_DB}.pools p
      JOIN ${CH_DB}.tokens tb ON tb.token_id = p.base_token_id
      JOIN ${CH_DB}.tokens tq ON tq.token_id = p.quote_token_id
      WHERE p.pair_contract = {pair_contract:String}
      LIMIT 1
    `,
    params: { pair_contract }
  });
  return rows?.[0] || null;
}

export async function getPoolId(pair_contract) {
  if (!pair_contract) return null;
  const cachedMeta = poolMetaCache.get(pair_contract);
  if (cachedMeta?.pool_id) return cachedMeta.pool_id;
  const cached = poolIdCache.get(pair_contract);
  if (cached) return cached;
  if (!redis) throw new Error('redis client not initialized in pool resolver');

  let attempts = 0;
  while (attempts < MAX_RETRIES) {
    const val = await redis.get(`${KEY_PREFIX}${pair_contract}`);
    const num = Number(val);
    if (Number.isFinite(num) && num > 0) {
      remember(poolIdCache, pair_contract, num);
      return num;
    }
    attempts++;
    await sleep(RETRY_MS);
  }
  return null;
}

export async function getPoolMeta(pair_contract) {
  if (!pair_contract) return null;
  const cached = poolMetaCache.get(pair_contract);
  if (cached) return cached;

  let attempts = 0;
  while (attempts < MAX_RETRIES) {
    // skipNegativeCache=true so we keep retrying right after pool creation
    const meta = await poolWithTokensCached(pair_contract, { skipNegativeCache: true });
    if (meta && meta.pool_id) {
      remember(poolIdCache, pair_contract, meta.pool_id);
      remember(poolMetaCache, pair_contract, meta);
      return meta;
    }

    // Fallback to ClickHouse if Timescale cache/db not yet populated
    const chMeta = await fetchPoolMetaClickhouse(pair_contract);
    if (chMeta && chMeta.pool_id) {
      remember(poolIdCache, pair_contract, chMeta.pool_id);
      remember(poolMetaCache, pair_contract, chMeta);
      return chMeta;
    }

    attempts++;
    await sleep(RETRY_MS);
  }
  return null;
}
