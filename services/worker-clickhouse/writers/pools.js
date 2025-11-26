
// services/worker-clickhouse/writers/pools.js
import { chInsertJSON } from '../../../common/db-clickhouse.js';
import { upsertPool, poolWithTokens } from '../../../common/core/pools.js';
import { info, warn } from '../../../common/log.js';

const buffer = [];
const MAX_BUFFER = Number(process.env.CLICKHOUSE_POOL_BUFFER || 200);
const FLUSH_MS   = Number(process.env.CLICKHOUSE_POOL_FLUSH_MS || 2000);

function asDate(v) {
  const d = new Date(v);
  return isNaN(d.getTime()) ? new Date() : d;
}

async function pushPool(row) {
  buffer.push(row);
  if (buffer.length >= MAX_BUFFER) {
    await flushPools();
  }
}

export async function flushPools() {
  if (!buffer.length) return;
  const batch = buffer.splice(0, buffer.length);
  await chInsertJSON({ table: 'pools', rows: batch });
}

export async function handlePoolEvent(e) {
  try {
    await upsertPool({
      pairContract: e.pair_contract,
      baseDenom: e.base_denom,
      quoteDenom: e.quote_denom,
      pairType: e.pair_type,
      createdAt: e.created_at,
      height: e.height,
      txHash: e.tx_hash,
      signer: e.signer
    });

    const pool = await poolWithTokens(e.pair_contract);
    if (!pool) return;

    await pushPool({
      pool_id: pool.pool_id,
      pair_contract: e.pair_contract,
      base_token_id: pool.base_id,
      quote_token_id: pool.quote_id,
      lp_token_denom: '',
      pair_type: e.pair_type || 'xyk',
      is_uzig_quote: pool.is_uzig_quote ? 1 : 0,
      factory_contract: process.env.FACTORY_ADDR || '',
      router_contract: process.env.ROUTER_ADDR || '',
      created_at: asDate(e.created_at),
      created_height: Number(e.height || 0),
      created_tx_hash: e.tx_hash || '',
      signer: e.signer || ''
    });

    info('[ch] pool recorded', e.pair_contract, pool.pool_id);
  } catch (err) {
    warn('[ch/pool]', err?.message || err);
  }
}

setInterval(() => { flushPools().catch(()=>{}); }, FLUSH_MS);
