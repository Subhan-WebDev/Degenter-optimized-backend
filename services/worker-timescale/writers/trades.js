// services/worker-timescale/writers/trades.js
import { poolWithTokensCached } from '../../../common/core/pools_cache.js';
import { insertTrade, drainTrades } from '../../../common/core/trades.js';
import { upsertPoolState } from '../../../common/core/pool_state.js';
import { upsertOHLCV1m } from '../../../common/core/ohlcv.js';
import { upsertPrice, priceFromReserves_UZIGQuote } from '../../../common/core/prices.js';
import { classifyDirection } from '../../../common/core/parse.js';
import { warn, debug } from '../../../common/log.js';

const SHOULD_DRAIN_NOW = process.env.STRICT_TRADES === '1';

export async function handleSwapEvent(e) {
  try {
    const pool = await poolWithTokensCached(e.pair_contract);
    if (!pool) {
      // only warn rarely (negative cache in pools_cache throttles further spam)
      warn('[ts/swap] unknown pool', e.pair_contract,e.height);
      return;
    }

    const direction = classifyDirection(e.offer_asset_denom, pool.quote_denom);

    const payload = {
      pool_id: pool.pool_id,
      pair_contract: e.pair_contract,
      action: 'swap',
      direction,
      offer_asset_denom: e.offer_asset_denom,
      offer_amount_base: e.offer_amount_base,
      ask_asset_denom:   e.ask_asset_denom,
      ask_amount_base:   e.ask_amount_base,
      return_amount_base: e.return_amount_base,
      is_router: !!e.is_router,
      reserve_asset1_denom: e.reserve_asset1_denom,
      reserve_asset1_amount_base: e.reserve_asset1_amount_base,
      reserve_asset2_denom: e.reserve_asset2_denom,
      reserve_asset2_amount_base: e.reserve_asset2_amount_base,
      height: e.height,
      tx_hash: e.tx_hash,
      signer: e.signer,
      msg_index: e.msg_index,
      event_index: e.event_index ?? 0,
      created_at: e.created_at
    };

    debug('[ts/swap] insert payload', payload);
    await insertTrade(payload);
    if (SHOULD_DRAIN_NOW) await drainTrades();

    // live pool state
    await upsertPoolState(
      pool.pool_id,
      pool.base_denom,
      pool.quote_denom,
      e.reserve_asset1_denom,
      e.reserve_asset1_amount_base,
      e.reserve_asset2_denom,
      e.reserve_asset2_amount_base
    );

    // ohlcv + price if UZIG quote and reserves are present
    if (pool.is_uzig_quote &&
        e.reserve_asset1_denom && e.reserve_asset1_amount_base &&
        e.reserve_asset2_denom && e.reserve_asset2_amount_base) {

      const reserves = [
        { denom: e.reserve_asset1_denom, amount_base: e.reserve_asset1_amount_base },
        { denom: e.reserve_asset2_denom, amount_base: e.reserve_asset2_amount_base }
      ];

      const price = priceFromReserves_UZIGQuote(
        { base_denom: pool.base_denom, base_exp: Number(pool.base_exp) },
        reserves
      );

      if (price && Number.isFinite(price) && price > 0) {
        const bucket = new Date(Math.floor(new Date(e.created_at).getTime() / 60000) * 60000);
        const quoteRaw = (e.offer_asset_denom === pool.quote_denom)
          ? Number(e.offer_amount_base || 0)
          : Number(e.return_amount_base || 0);
        const volZig = quoteRaw / 1e6;

        await upsertOHLCV1m({
          pool_id: pool.pool_id,
          bucket_start: bucket,
          price,
          vol_zig: volZig,
          trade_inc: 1
        });

        // IMPORTANT: your prices table expects token_id; use the base token id
        await upsertPrice(pool.base_token_id, pool.pool_id, price, true);
      }
    }
  } catch (err) {
    warn('[ts/swap]', err?.message || err);
  }
}

export async function handleLiquidityEvent(e) {
  try {
    const pool = await poolWithTokensCached(e.pair_contract);
    if (!pool) { warn('[ts/liq] unknown pool', e.pair_contract, e.height); return; }

    const payload = {
      pool_id: pool.pool_id,
      pair_contract: e.pair_contract,
      action: e.action,
      direction: e.action,
      offer_asset_denom: null,
      offer_amount_base: null,
      ask_asset_denom: null,
      ask_amount_base: null,
      return_amount_base: e.share_base || null,
      is_router: false,
      reserve_asset1_denom: e.reserve_asset1_denom,
      reserve_asset1_amount_base: e.reserve_asset1_amount_base,
      reserve_asset2_denom: e.reserve_asset2_denom,
      reserve_asset2_amount_base: e.reserve_asset2_amount_base,
      height: e.height,
      tx_hash: e.tx_hash,
      signer: e.signer,
      msg_index: e.msg_index,
      event_index: e.event_index ?? 0,
      created_at: e.created_at
    };

    debug('[ts/liq] insert payload', payload);
    await insertTrade(payload);
    if (SHOULD_DRAIN_NOW) await drainTrades();

    await upsertPoolState(
      pool.pool_id,
      pool.base_denom,
      pool.quote_denom,
      e.reserve_asset1_denom,
      e.reserve_asset1_amount_base,
      e.reserve_asset2_denom,
      e.reserve_asset2_amount_base
    );
  } catch (err) {
    warn('[ts/liq]', err?.message || err);
  }
}
