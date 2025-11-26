import 'dotenv/config';
import { createRedisClient } from '../../../common/redis-client.js';
import { createConsumerGroup, readLoop } from '../../../common/streams.js';
import { TS as DB } from '../../../common/db-timescale.js';
import { info, warn, debug } from '../../../common/log.js';

import { refreshMetaOnce } from './meta-refresher.js';
import { refreshHoldersOnce } from './holders-refresher.js';
import { scanTokenOnce } from './token-security.js';
import {
  refreshPoolMatrixOnce,
  refreshTokenMatrixOnce,
} from './matrix-rollups.js';

import {
  fetchPoolReserves,
  priceFromReserves_UZIGQuote,
  upsertPrice,
} from '../../../common/core/prices.js';

// --- Stream + group names (accept multiple env names, in priority order) ---
const STREAM =
  process.env.FT_STREAM ||
  process.env.STREAM_NEW_POOL ||
  'events:new_pool';

const GROUP =
  process.env.FT_GROUP ||
  process.env.FASTTRACK_GROUP ||
  'fasttrack';

const BATCH = Number(process.env.FASTTRACK_BATCH || 64);
const BLOCK_MS = Number(process.env.FASTTRACK_BLOCK_MS || 1000);

// Timeouts (ms) for things we DO await
const SECURITY_TIMEOUT_MS = Number(
  process.env.FASTTRACK_SECURITY_TIMEOUT_MS || 15000,
);

// uzig denom (we don’t want to touch its holders in fasttrack)
const UZIG_DENOM = process.env.UZIG_DENOM || 'uzig';
const isUzigDenom = (d) => d === UZIG_DENOM;

const { client: redis, connect: redisConnect } = createRedisClient('fasttrack');

async function getPoolByPair(pair_contract) {
  const { rows } = await DB.query(
    `
    SELECT p.pool_id, p.base_token_id, p.quote_token_id, p.is_uzig_quote, p.created_at
    FROM pools p
    WHERE p.pair_contract = $1
  `,
    [pair_contract],
  );
  return rows[0] || null;
}

async function getToken(token_id) {
  const { rows } = await DB.query(
    `SELECT token_id, denom, exponent FROM tokens WHERE token_id = $1`,
    [token_id],
  );
  return rows[0] || null;
}

function floorToMinute(ts) {
  const d = ts ? new Date(ts) : new Date();
  d.setSeconds(0, 0);
  return d;
}

async function seedOhlcvOneMinute(pool_id, priceInZig, created_at) {
  if (!pool_id || !priceInZig) return;
  const bucket = floorToMinute(created_at);
  await DB.query(
    `
    INSERT INTO ohlcv_1m(
      pool_id,
      bucket_start,
      open,
      high,
      low,
      close,
      volume_zig,
      trade_count,
      liquidity_zig
    )
    VALUES ($1, $2, $3, $3, $3, $3, 0, 0, NULL)
    ON CONFLICT (pool_id, bucket_start) DO NOTHING
  `,
    [pool_id, bucket, priceInZig],
  );
}

async function upsertPoolState(pool_id, reserve_base_base, reserve_quote_base) {
  if (!pool_id) return;
  await DB.query(
    `
    INSERT INTO pool_state(
      pool_id,
      reserve_base_base,
      reserve_quote_base,
      updated_at
    )
    VALUES ($1, $2, $3, now())
    ON CONFLICT (pool_id) DO UPDATE SET
      reserve_base_base  = EXCLUDED.reserve_base_base,
      reserve_quote_base = EXCLUDED.reserve_quote_base,
      updated_at         = now()
  `,
    [
      pool_id,
      String(reserve_base_base || 0),
      String(reserve_quote_base || 0),
    ],
  );
}

/**
 * Wrap a promise with a timeout for things we DO await (security).
 */
function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(
        () => reject(new Error(`${label} timeout after ${ms}ms`)),
        ms,
      ),
    ),
  ]);
}

async function runFasttrack(payload) {
  // payload comes from processor-core's out.pools object
  // expected fields: pair_contract, base_token_id/denom (maybe), quote_token_id/denom (maybe),
  // created_at, tx_hash, height, msg_index
  const { pair_contract, created_at } = payload || {};
  if (!pair_contract) {
    warn('[fasttrack] missing pair_contract in payload');
    return;
  }

  debug('[fasttrack] got new_pool', {
    pair_contract,
    created_at,
    tx: payload?.tx_hash,
    height: payload?.height,
  });

  // Verify pool exists in DB (worker-timescale should upsert pools very quickly)
  let p = await getPoolByPair(pair_contract);
  if (!p) {
    // Not in DB yet — can happen if metadata worker outruns timescale writer by a few ms.
    // Small retry loop:
    for (let i = 0; i < 8; i++) {
      await new Promise((r) => setTimeout(r, 150));
      p = await getPoolByPair(pair_contract);
      if (p) break;
    }
    if (!p) {
      warn('[fasttrack] pool not found after retries', pair_contract);
      return;
    }
  }

  const base = await getToken(p.base_token_id);
  const quote = await getToken(p.quote_token_id);
  if (!base || !quote) {
    warn('[fasttrack] token rows missing', {
      base_id: p.base_token_id,
      quote_id: p.quote_token_id,
    });
    return;
  }

  /* -------------------- 1) META -------------------- */

  info('[fasttrack] step=meta:start', {
    pair_contract,
    base: base.denom,
    quote: quote.denom,
  });

  await Promise.allSettled([
    refreshMetaOnce(base.denom),
    refreshMetaOnce(quote.denom),
  ]);

  info('[fasttrack] step=meta:done', {
    pair_contract,
    base: base.denom,
    quote: quote.denom,
  });

  /* -------------------- 2) HOLDERS (fire-and-forget) -------------------- */

  info('[fasttrack] step=holders:fire', {
    pair_contract,
    base: base.denom,
    quote: quote.denom,
  });

  // base token holders (usually the new token) – we kick it off but do NOT await
  if (!isUzigDenom(base.denom)) {
    refreshHoldersOnce(base.token_id, base.denom, 10).catch((e) =>
      warn('[fasttrack/holders]', base.denom, e?.message || e),
    );
  } else {
    info('[fasttrack] holders:skip-uzig', { denom: base.denom });
  }

  // quote token holders — we very specifically do not want to ever block on uzig here
  if (!isUzigDenom(quote.denom)) {
    refreshHoldersOnce(quote.token_id, quote.denom, 10).catch((e) =>
      warn('[fasttrack/holders]', quote.denom, e?.message || e),
    );
  } else {
    info('[fasttrack] holders:skip-uzig', { denom: quote.denom });
  }

  // we immediately move on; background holders worker + these fire-and-forget
  info('[fasttrack] step=holders:done', {
    pair_contract,
    base: base.denom,
    quote: quote.denom,
  });

  /* -------------------- 3) SECURITY (await, with timeout) -------------------- */

  info('[fasttrack] step=security:start', {
    pair_contract,
    base: base.denom,
    quote: quote.denom,
  });

  const securityTasks = [];

  // care mainly about new token (base), but we also optionally scan quote if it’s not uzig
  if (!isUzigDenom(base.denom)) {
    securityTasks.push(
      withTimeout(
        scanTokenOnce(base.token_id, base.denom),
        SECURITY_TIMEOUT_MS,
        `security ${base.denom}`,
      ).catch((e) =>
        warn('[fasttrack/security]', base.denom, e?.message || e),
      ),
    );
  } else {
    info('[fasttrack] security:skip-uzig', { denom: base.denom });
  }

  if (!isUzigDenom(quote.denom)) {
    securityTasks.push(
      withTimeout(
        scanTokenOnce(quote.token_id, quote.denom),
        SECURITY_TIMEOUT_MS,
        `security ${quote.denom}`,
      ).catch((e) =>
        warn('[fasttrack/security]', quote.denom, e?.message || e),
      ),
    );
  } else {
    info('[fasttrack] security:skip-uzig', { denom: quote.denom });
  }

  if (securityTasks.length) {
    await Promise.allSettled(securityTasks);
  }

  info('[fasttrack] step=security:done', {
    pair_contract,
    base: base.denom,
    quote: quote.denom,
  });

  /* -------------------- 3.5) MATRIX -------------------- */

  info('[fasttrack] step=matrix:start', {
    pair_contract,
    pool_id: p.pool_id,
  });

  try {
    await Promise.allSettled([
      refreshPoolMatrixOnce(p.pool_id),
      refreshTokenMatrixOnce(base.token_id),
      refreshTokenMatrixOnce(quote.token_id),
    ]);
    info('[fasttrack] step=matrix:done', {
      pair_contract,
      pool_id: p.pool_id,
    });
  } catch (e) {
    warn('[fasttrack/matrix]', e?.message || e);
  }

  /* -------------------- 4) PRICE / STATE / OHLCV -------------------- */

  info('[fasttrack] step=price:start', { pair_contract, pool_id: p.pool_id });

  try {
    const reserves = await fetchPoolReserves(pair_contract);
    await upsertPoolState(
      p.pool_id,
      reserves.reserve_base_base,
      reserves.reserve_quote_base,
    );

    if (p.is_uzig_quote) {
      const price = priceFromReserves_UZIGQuote(
        { base_denom: base.denom, base_exp: Number(base.exponent ?? 6) },
        reserves,
      );
      if (price && Number.isFinite(price) && price > 0) {
        await upsertPrice(base.token_id, p.pool_id, price, true);
        await seedOhlcvOneMinute(
          p.pool_id,
          price,
          created_at || p.created_at || new Date(),
        );
        debug('[fasttrack] seeded price & 1m OHLCV', {
          pool_id: p.pool_id,
          price,
        });
      }
    }

    info('[fasttrack] step=price:done', {
      pair_contract,
      pool_id: p.pool_id,
    });
  } catch (e) {
    warn('[fasttrack] reserves/price', e?.message || e);
  }

  info('[fasttrack] completed', pair_contract);
}

export async function startFasttrackConsumer() {
  await redisConnect();

  // Create/read group
  await createConsumerGroup({ redis, stream: STREAM, group: GROUP });
  const consumer = `${
    process.env.SVC_NAME || 'worker-metadata'
  }-fast-${Math.random().toString(36).slice(2, 6)}`;

  info('[fasttrack] listening', {
    stream: STREAM,
    group: GROUP,
    consumer,
    batch: BATCH,
    blockMs: BLOCK_MS,
  });

  return readLoop({
    redis,
    stream: STREAM,
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
          await runFasttrack(obj);
        } catch (e) {
          warn('[fasttrack] handler', e?.message || e);
        }
      }
      if (ids.length) await ackMany(ids);
    },
  });
}

export default { startFasttrackConsumer };
