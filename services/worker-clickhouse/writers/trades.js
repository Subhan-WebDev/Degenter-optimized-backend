import { chInsertJSON, toChDateTime } from '../../../common/db-clickhouse.js';
import BatchQueue from '../../../common/batch.js';
import { classifyDirection } from '../../../common/core/parse.js';
import { priceFromReserves_UZIGQuote } from '../../../common/core/prices.js';
import { info, warn } from '../../../common/log.js';
import { getPoolMeta } from '../pool_resolver.js';
import { pushPoolState } from './pool_state.js';
import { pushTick } from './prices.js';

const tradesQueue = new BatchQueue({
  maxItems: Number(process.env.CLICKHOUSE_TRADE_BUFFER || 1000),
  maxWaitMs: Number(process.env.CLICKHOUSE_TRADE_FLUSH_MS || 2000),
  flushFn: flushTradesBatch
});

const RETRY_DELAY_MS = Number(process.env.CLICKHOUSE_META_RETRY_MS || 500);
const MAX_META_RETRIES = Number(process.env.CLICKHOUSE_META_RETRIES || 120); // ~1 min default
const TRADE_SEEN_MAX = Number(process.env.CLICKHOUSE_TRADE_SEEN_MAX || 100_000);
const tradeSeen = new Set(); // process-level dedupe to avoid duplicate inserts

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
  const ev = BigInt(e?.event_index ?? 0);
  const hashSuffix = BigInt(parseInt((e?.tx_hash || '').slice(-4), 16) || 0); // 0..65535
  // Compose a stable UInt64: height * 1e9 + msg * 1e6 + event * 1e4 + hashSuffix
  return (h * 1_000_000_000n + m * 1_000_000n + ev * 10_000n + hashSuffix).toString();
}

function bucketStart(ts) {
  const t = asDate(ts).getTime();
  return toChDateTime(Math.floor(t / 60000) * 60000);
}

function rememberTradeId(id) {
  tradeSeen.add(id);
  if (tradeSeen.size > TRADE_SEEN_MAX) {
    const first = tradeSeen.values().next().value;
    tradeSeen.delete(first);
  }
}

function computePrice(meta, e) {
  if (!meta?.is_uzig_quote) return null;
  if (!e.reserve_asset1_denom || !e.reserve_asset1_amount_base || !e.reserve_asset2_denom || !e.reserve_asset2_amount_base) return null;
  const reserves = [
    { denom: e.reserve_asset1_denom, amount_base: e.reserve_asset1_amount_base },
    { denom: e.reserve_asset2_denom, amount_base: e.reserve_asset2_amount_base }
  ];
  return priceFromReserves_UZIGQuote({ base_denom: meta.base_denom, base_exp: Number(meta.base_exp) }, reserves);
}

function computeVolumeZig(meta, e) {
  if (!meta?.quote_denom) return '0';
  const quoteRaw = (e.offer_asset_denom === meta.quote_denom)
    ? Number(e.offer_amount_base || 0)
    : Number(e.return_amount_base || 0);
  if (!Number.isFinite(quoteRaw) || quoteRaw <= 0) return '0';
  return (quoteRaw / 1e6).toFixed(8);
}

async function flushTradesBatch(events) {
  if (!events.length) return;

  const tradeRows = [];
  const ohlcvAgg = new Map(); // key: `${pool_id}-${bucket_start}` -> aggregated ohlcv row
  let queuedForRetry = 0;

  for (const e of events) {
    try {
      if (!e?.pair_contract) { warn('[ch/trade] missing pair_contract on event'); continue; }

      const meta = await getPoolMeta(e.pair_contract);
      if (!meta) {
        if ((e._metaRetries || 0) < MAX_META_RETRIES) {
          if ((e._metaRetries || 0) === 0 || (e._metaRetries + 1) % 10 === 0) {
            warn('[ch/trade] waiting for pool metadata', { pair_contract: e.pair_contract, attempt: (e._metaRetries || 0) + 1 });
          }
          setTimeout(() => tradesQueue.push({ ...e, _metaRetries: (e._metaRetries || 0) + 1 }), RETRY_DELAY_MS);
          queuedForRetry++;
        } else {
          warn('[ch/trade] missing pool meta (dropped)', e.pair_contract);
        }
        continue;
      }

      const direction = classifyDirection(e.offer_asset_denom, meta.quote_denom);
      const tradeId = nextTradeId(e);
      if (tradeSeen.has(tradeId)) continue;

      tradeRows.push({
        trade_id: tradeId,
        pool_id: meta.pool_id,
        pair_contract: e.pair_contract,
        action: e.action || 'swap',
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
        created_at: toChDateTime(e.created_at)
      });

      // pool_state snapshot for analytics
      const baseRes = (e.reserve_asset1_denom === meta.base_denom)
        ? e.reserve_asset1_amount_base
        : (e.reserve_asset2_denom === meta.base_denom ? e.reserve_asset2_amount_base : null);
      const quoteRes = (e.reserve_asset1_denom === meta.quote_denom)
        ? e.reserve_asset1_amount_base
        : (e.reserve_asset2_denom === meta.quote_denom ? e.reserve_asset2_amount_base : null);
      if (baseRes != null && quoteRes != null) {
        pushPoolState({
          pool_id: meta.pool_id,
          reserve_base_base: toDecimal(baseRes),
          reserve_quote_base: toDecimal(quoteRes),
          updated_at: toChDateTime(e.created_at)
        });
      }

      const price = computePrice(meta, e);
      if (price && Number.isFinite(price) && price > 0) {
        const volZig = computeVolumeZig(meta, e);
        const bucket = bucketStart(e.created_at);
        const key = `${meta.pool_id}-${bucket}`;
        const existing = ohlcvAgg.get(key);
        if (existing) {
          existing.high = Math.max(existing.high, price);
          existing.low = Math.min(existing.low, price);
          existing.close = price;
          existing.volume_zig = (Number(existing.volume_zig) + Number(volZig)).toString();
          existing.trade_count += 1;
        } else {
          ohlcvAgg.set(key, {
            pool_id: meta.pool_id,
            bucket_start: bucket,
            open: price,
            high: price,
            low: price,
            close: price,
            volume_zig: volZig,
            trade_count: 1,
            liquidity_zig: '0'
          });
        }

        pushTick({
          pool_id: meta.pool_id,
          token_id: meta.base_token_id,
          price_in_zig: price,
          ts: toChDateTime(e.created_at)
        });
      }

      rememberTradeId(tradeId);
    } catch (err) {
      warn('[ch/trade/flush]', err?.message || err);
    }
  }

  if (queuedForRetry) {
    info('[ch/trade] queued for meta retry', { count: queuedForRetry });
  }

  if (tradeRows.length) {
    await chInsertJSON({
      table: 'trades',
      rows: tradeRows,
      settings: { deduplicate_by_primary_key: 1 }
    });
  }
  if (ohlcvAgg.size) {
    await chInsertJSON({
      table: 'ohlcv_1m',
      rows: Array.from(ohlcvAgg.values())
    });
  }
}

export async function flushTrades() {
  await tradesQueue.drain();
}

export async function handleSwapEvent(e) {
  if (!e?.pair_contract) { warn('[ch/swap] missing pair_contract on event'); return true; }
  tradesQueue.push({ ...e, action: 'swap' });
}

export async function handleLiquidityEvent(e) {
  if (!e?.pair_contract) { warn('[ch/liq] missing pair_contract on event'); return true; }
  tradesQueue.push({ ...e, action: e.action || 'liquidity' });
}
