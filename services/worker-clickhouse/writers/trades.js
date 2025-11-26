import { chInsertJSON } from '../../../common/db-clickhouse.js';
import { poolWithTokens } from '../../../common/core/pools.js';
import { classifyDirection } from '../../../common/core/parse.js';
import { warn } from '../../../common/log.js';
import { pushPoolState } from './pool_state.js';

const tradesBuffer = [];
const MAX_BUFFER = Number(process.env.CLICKHOUSE_TRADE_BUFFER || 1000);
const FLUSH_MS   = Number(process.env.CLICKHOUSE_TRADE_FLUSH_MS || 2000);

function asDate(v) {
  if (!v) return new Date();
  const d = new Date(v);
  return isNaN(d.getTime()) ? new Date() : d;
}

function toDecimal(v) {
  if (v === null || v === undefined) return '0';
  return String(v);
}

function nextTradeId(e) {
  const h = BigInt(e?.height || 0);
  const m = BigInt(e?.msg_index ?? 0);
  return (h * 1_000_000n + m).toString();
}

async function pushTrade(row) {
  tradesBuffer.push(row);
  if (tradesBuffer.length >= MAX_BUFFER) {
    await flushTrades();
  }
}

export async function flushTrades() {
  if (!tradesBuffer.length) return;
  const batch = tradesBuffer.splice(0, tradesBuffer.length);
  await chInsertJSON({ table: 'trades', rows: batch });
}

async function buildPoolContext(pair_contract) {
  const pool = await poolWithTokens(pair_contract);
  if (!pool) throw new Error(`unknown pool ${pair_contract}`);
  return pool;
}

function mapReservesToBaseQuote(pool, e) {
  const base = { denom: pool.base_denom, amount: null };
  const quote = { denom: pool.quote_denom, amount: null };

  if (e.reserve_asset1_denom === pool.base_denom) base.amount = e.reserve_asset1_amount_base;
  if (e.reserve_asset2_denom === pool.base_denom) base.amount = e.reserve_asset2_amount_base;
  if (e.reserve_asset1_denom === pool.quote_denom) quote.amount = e.reserve_asset1_amount_base;
  if (e.reserve_asset2_denom === pool.quote_denom) quote.amount = e.reserve_asset2_amount_base;

  return { base, quote };
}

export async function handleSwapEvent(e) {
  try {
    const pool = await buildPoolContext(e.pair_contract);
    const direction = classifyDirection(e.offer_asset_denom, pool.quote_denom);

    await pushTrade({
      trade_id: nextTradeId(e),
      pool_id: pool.pool_id,
      pair_contract: e.pair_contract,
      action: 'swap',
      direction,
      offer_asset_denom: e.offer_asset_denom || '',
      offer_amount_base: toDecimal(e.offer_amount_base),
      ask_asset_denom: e.ask_asset_denom || '',
      ask_amount_base: toDecimal(e.ask_amount_base),
      return_amount_base: toDecimal(e.return_amount_base),
      is_router: e.is_router ? 1 : 0,
      reserve_asset1_denom: e.reserve_asset1_denom || '',
      reserve_asset1_amount_base: toDecimal(e.reserve_asset1_amount_base),
      reserve_asset2_denom: e.reserve_asset2_denom || '',
      reserve_asset2_amount_base: toDecimal(e.reserve_asset2_amount_base),
      height: Number(e.height || 0),
      tx_hash: e.tx_hash || '',
      signer: e.signer || '',
      msg_index: Number(e.msg_index || 0),
      created_at: asDate(e.created_at)
    });

    const reserves = mapReservesToBaseQuote(pool, e);
    if (reserves.base.amount != null && reserves.quote.amount != null) {
      pushPoolState({
        pool_id: pool.pool_id,
        reserve_base_base: toDecimal(reserves.base.amount),
        reserve_quote_base: toDecimal(reserves.quote.amount),
        updated_at: asDate(e.created_at)
      });
    }
  } catch (err) {
    warn('[ch/swap]', err?.message || err);
  }
}

export async function handleLiquidityEvent(e) {
  try {
    const pool = await buildPoolContext(e.pair_contract);
    const isProvide = e.action === 'provide';
    const direction = isProvide ? 'provide' : 'withdraw';

    await pushTrade({
      trade_id: nextTradeId(e),
      pool_id: pool.pool_id,
      pair_contract: e.pair_contract,
      action: e.action,
      direction,
      offer_asset_denom: '',
      offer_amount_base: '0',
      ask_asset_denom: '',
      ask_amount_base: '0',
      return_amount_base: toDecimal(e.share_base),
      is_router: 0,
      reserve_asset1_denom: e.reserve_asset1_denom || '',
      reserve_asset1_amount_base: toDecimal(e.reserve_asset1_amount_base),
      reserve_asset2_denom: e.reserve_asset2_denom || '',
      reserve_asset2_amount_base: toDecimal(e.reserve_asset2_amount_base),
      height: Number(e.height || 0),
      tx_hash: e.tx_hash || '',
      signer: e.signer || '',
      msg_index: Number(e.msg_index || 0),
      created_at: asDate(e.created_at)
    });

    const reserves = mapReservesToBaseQuote(pool, e);
    if (reserves.base.amount != null && reserves.quote.amount != null) {
      pushPoolState({
        pool_id: pool.pool_id,
        reserve_base_base: toDecimal(reserves.base.amount),
        reserve_quote_base: toDecimal(reserves.quote.amount),
        updated_at: asDate(e.created_at)
      });
    }
  } catch (err) {
    warn('[ch/liq]', err?.message || err);
  }
}

setInterval(() => { flushTrades().catch(()=>{}); }, FLUSH_MS);