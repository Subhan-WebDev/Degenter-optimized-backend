// services/worker-clickhouse/pool_resolver.js
import { poolWithTokensCached } from '../../common/core/pools_cache.js';

const CACHE_MAX = Number(process.env.POOL_CACHE_MAX || 2000);
const RETRY_MS = Number(process.env.POOL_ID_RETRY_MS || 500);
const MAX_RETRIES = Number(process.env.POOL_ID_MAX_RETRIES || 120); // wait up to ~1m by default
const KEY_PREFIX = process.env.REDIS_POOL_PREFIX || 'pool_id:';

let redis = null;

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

export async function getPoolId(pair_contract) {
  if (!pair_contract) return null;
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

  const pool_id = await getPoolId(pair_contract);
  if (!pool_id) return null;

  let attempts = 0;
  while (attempts < MAX_RETRIES) {
    const meta = await poolWithTokensCached(pair_contract);
    if (meta && meta.pool_id) {
      remember(poolIdCache, pair_contract, meta.pool_id);
      remember(poolMetaCache, pair_contract, meta);
      return meta;
    }
    attempts++;
    await sleep(RETRY_MS);
  }
  return null;
}
