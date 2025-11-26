// services/worker-clickhouse/writers/prices.js
import { chInsertJSON, toChDateTime } from '../../../common/db-clickhouse.js';
import BatchQueue from '../../../common/batch.js';
import { priceFromReserves_UZIGQuote } from '../../../common/core/prices.js';
import { warn } from '../../../common/log.js';
import { getPoolMeta } from '../pool_resolver.js';

const priceQueue = new BatchQueue({
  maxItems: Number(process.env.CLICKHOUSE_PRICE_BUFFER || 1000),
  maxWaitMs: Number(process.env.CLICKHOUSE_PRICE_FLUSH_MS || 2000),
  flushFn: async (items) => {
    if (items.length) {
      await chInsertJSON({ table: 'price_ticks', rows: items });
    }
  }
});

export function pushTick(row) {
  priceQueue.push(row);
}

export async function flushPriceTicks() {
  await priceQueue.drain();
}

export async function handlePriceSnapshot(e) {
  if (e?.kind !== 'reserves_snapshot') return;

  try {
    const meta = await getPoolMeta(e.pair_contract);
    if (!meta || !meta.is_uzig_quote) return;

    const price = priceFromReserves_UZIGQuote(
      { base_denom: meta.base_denom, base_exp: Number(meta.base_exp) },
      e.reserves || []
    );

    if (price != null && Number.isFinite(price) && price > 0) {
      pushTick({
        pool_id: meta.pool_id,
        token_id: meta.base_token_id,
        price_in_zig: price,
        ts: toChDateTime(e.at || e.created_at)
      });
    }
  } catch (err) {
    warn('[ch/price]', err?.message || err);
  }
}
