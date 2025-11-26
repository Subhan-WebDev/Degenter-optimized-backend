// services/worker-timescale/writers/prices.js
import { poolWithTokens } from '../../../common/core/pools.js';
import { upsertPrice, priceFromReserves_UZIGQuote } from '../../../common/core/prices.js';
import { warn, info } from '../../../common/log.js';

/**
 * price snapshot event (from processor):
 * {
 *   kind: 'reserves_snapshot',
 *   pair_contract,
 *   reserves: [{denom, amount_base}, {denom, amount_base}],
 *   at, height, tx_hash, source
 * }
 *
 * We only update the current price here (no OHLCV to avoid double-counting volume).
 * OHLCV is updated in the swap writer where we know vol_zig and trade_inc.
 */
export async function handlePriceSnapshot(e) {
  if (e?.kind !== 'reserves_snapshot') return;

  try {
    const pool = await poolWithTokens(e.pair_contract);
    if (!pool) return;
    if (!pool.is_uzig_quote) return;

    const price = priceFromReserves_UZIGQuote(
      { base_denom: pool.base_denom, base_exp: Number(pool.base_exp) },
      e.reserves || []
    );
    if (price != null && Number.isFinite(price) && price > 0) {
      await upsertPrice(pool.base_id, pool.pool_id, price, true);
    }
  } catch (err) {
    warn('[ts/price]', err?.message || err);
  }
}
