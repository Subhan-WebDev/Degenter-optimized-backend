// common/core/pools.js
import { DB } from '../db-timescale.js';
import { info } from '../log.js';
import { putPool, invalidatePool, poolWithTokensCached } from './pools_cache.js'; // ← add import

/** Minimal token upsert that matches your DDL (denom UNIQUE). */
async function upsertTokenMinimal(denom, exp = 6) {
  const sql = `
    INSERT INTO public.tokens (denom, exponent)
    VALUES ($1, $2)
    ON CONFLICT (denom) DO UPDATE SET exponent = public.tokens.exponent
    RETURNING token_id, exponent
  `;
  const { rows } = await DB.query(sql, [denom, exp]);
  return rows[0]?.token_id;
}

export async function upsertPool({
  pairContract, baseDenom, quoteDenom, pairType,
  createdAt, height, txHash, signer
}) {
  const baseId  = await upsertTokenMinimal(baseDenom, 6);
  const quoteId = await upsertTokenMinimal(quoteDenom, 6);
  const isUzig  = (quoteDenom === (process.env.UZIG_DENOM || 'uzig'));

  const sql = `
    INSERT INTO public.pools
      (pair_contract, base_token_id, quote_token_id, pair_type, is_uzig_quote,
       created_at, created_height, created_tx_hash, signer)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
    ON CONFLICT (pair_contract) DO UPDATE SET
      base_token_id = EXCLUDED.base_token_id,
      quote_token_id = EXCLUDED.quote_token_id,
      pair_type = EXCLUDED.pair_type,
      is_uzig_quote = EXCLUDED.is_uzig_quote
    RETURNING pool_id
  `;
  const { rows } = await DB.query(sql, [
    pairContract, baseId, quoteId, String(pairType), isUzig,
    createdAt, height, txHash, signer
  ]);
  const pool_id = rows[0].pool_id;

  putPool({
    pool_id,
    pair_contract: pairContract,
    is_uzig_quote: isUzig,
    base_token_id: baseId,
    quote_token_id: quoteId,
    base_denom: baseDenom,
    quote_denom: quoteDenom,
    base_exp: 6,
    quote_exp: 6,
  });

  info('POOL UPSERT:', pairContract, `${baseDenom}/${quoteDenom}`, pairType, 'pool_id=', pool_id);
  return { pool_id, base_token_id: baseId, quote_token_id: quoteId };
}

/** 🔁 Back-compat for older imports */
export async function poolWithTokens(pairContract) {
  return poolWithTokensCached(pairContract);
}

export { invalidatePool }; // keep exporting if others import it
