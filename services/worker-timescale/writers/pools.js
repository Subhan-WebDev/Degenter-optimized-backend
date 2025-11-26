// services/worker-timescale/writers/pools.js
import { upsertPool } from '../../../common/core/pools.js';
import { setTokenMetaFromLCD } from '../../../common/core/tokens.js';
import { putPool, invalidatePool } from '../../../common/core/pools_cache.js';
import { info, warn } from '../../../common/log.js';

/**
 * Event:
 * { pair_contract, base_denom, quote_denom, pair_type, created_at, height, tx_hash, signer }
 */
export async function handlePoolEvent(e) {
  try {
    // Invalidate any stale entry before upsert
    invalidatePool(e.pair_contract);

    const pool_id = await upsertPool({
      pairContract: e.pair_contract,
      baseDenom: e.base_denom,
      quoteDenom: e.quote_denom,
      pairType: e.pair_type,
      createdAt: e.created_at,
      height: e.height,
      txHash: e.tx_hash,
      signer: e.signer
    });

    // Optionally ensure cache has the final shape (in case tokens carried different exponents)
    putPool({
      pool_id,
      pair_contract: e.pair_contract,
      is_uzig_quote: (e.quote_denom === (process.env.UZIG_DENOM || 'uzig')),
      base_token_id: undefined,  // not needed by writers; upsertPool already set cache fully
      quote_token_id: undefined,
      base_denom: e.base_denom,
      quote_denom: e.quote_denom,
      base_exp: 6,
      quote_exp: 6,
    });

    // Fire & forget token metadata updates (no await to keep fast)
    setTokenMetaFromLCD(e.base_denom).catch(()=>{});
    setTokenMetaFromLCD(e.quote_denom).catch(()=>{});

    info('[ts] pool upsert', e.pair_contract, 'pool_id=', pool_id);
  } catch (err) {
    warn('[ts/pool]', err?.message || err);
  }
}
