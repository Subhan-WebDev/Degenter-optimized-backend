// common/core/pools_cache.js
import { DB } from '../db-timescale.js';
import { debug, warn, info } from '../log.js';

const POS = new Map(); // pair_contract -> { pool_id, pair_contract, base_token_id, quote_token_id, base_denom, quote_denom, base_exp, quote_exp, is_uzig_quote, _t }
const NEG = new Map(); // pair_contract -> expiresAt(ms)

const POOLS_NEG_TTL_MS  = Number(process.env.POOLS_NEG_TTL_MS  || 60_000);
const POOLS_SOFT_TTL_MS = Number(process.env.POOLS_SOFT_TTL_MS || 10 * 60_000);

/* ----------------------- internal helpers ----------------------- */
function now() { return Date.now(); }

async function fetchPoolRow(pair_contract) {
  const sql = `
    SELECT
      p.pool_id,
      p.pair_contract,
      p.is_uzig_quote,
      tb.token_id AS base_token_id, tb.denom AS base_denom, COALESCE(tb.exponent,6) AS base_exp,
      tq.token_id AS quote_token_id, tq.denom AS quote_denom, COALESCE(tq.exponent,6) AS quote_exp
    FROM public.pools p
    JOIN public.tokens tb ON tb.token_id = p.base_token_id
    JOIN public.tokens tq ON tq.token_id = p.quote_token_id
    WHERE p.pair_contract = $1
    LIMIT 1
  `;
  const { rows } = await DB.query(sql, [pair_contract]);
  return rows[0] || null;
}

/* ----------------------- API ----------------------- */
export function putPool(meta) {
  POS.set(meta.pair_contract, { ...meta, _t: now() });
  NEG.delete(meta.pair_contract);
}

export function invalidatePool(pair_contract) {
  POS.delete(pair_contract);
  NEG.delete(pair_contract);
}

export async function preloadPools() {
  const sql = `
    SELECT
      p.pool_id,
      p.pair_contract,
      p.is_uzig_quote,
      tb.token_id AS base_token_id, tb.denom AS base_denom, COALESCE(tb.exponent,6) AS base_exp,
      tq.token_id AS quote_token_id, tq.denom AS quote_denom, COALESCE(tq.exponent,6) AS quote_exp
    FROM public.pools p
    JOIN public.tokens tb ON tb.token_id = p.base_token_id
    JOIN public.tokens tq ON tq.token_id = p.quote_token_id
  `;
  try {
    const { rows } = await DB.query(sql, []);
    POS.clear(); NEG.clear();
    const t = now();
    for (const r of rows) POS.set(r.pair_contract, { ...r, _t: t });
    info('[pools-cache] preloaded', { count: rows.length });
  } catch (e) {
    warn('[pools-cache] preload error', e?.message || e);
  }
}

/**
 * Fast path: O(1) memory lookups.
 * Returns:
 *   - object   => cached pool row
 *   - null     => known-missing for a short time (negative cached)
 *   - fetch DB => if uncached; caches result or negative-caches
 */
export async function poolWithTokensCached(pair_contract, { skipNegativeCache = false } = {}) {
  const neg = NEG.get(pair_contract);
  if (!skipNegativeCache && neg && neg > now()) return null; // recent known-miss

  const hit = POS.get(pair_contract);
  if (hit) {
    // Soft TTL → still return immediately; if stale, refresh in background
    if (hit._t && (now() - hit._t) > POOLS_SOFT_TTL_MS) {
      // Fire-and-forget refresh (no await)
      fetchPoolRow(pair_contract)
        .then(row => { if (row) putPool(row); })
        .catch(() => {});
    }
    return hit;
  }

  // Not cached → single DB read
  const row = await fetchPoolRow(pair_contract);
  if (row) {
    putPool(row);
    return row;
  }

  // Miss → short negative cache to avoid DB spam
  if (!skipNegativeCache) {
    NEG.set(pair_contract, now() + POOLS_NEG_TTL_MS);
    debug('[pools-cache] miss', { pair_contract });
  }
  return null;
}
