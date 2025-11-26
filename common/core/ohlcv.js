// common/core/ohlcv.js
import { DB } from '../../common/db-timescale.js';
import BatchQueue from '../batch.js';

// Env toggles
const STRICT_OHLCV    = process.env.STRICT_OHLCV === '1';      // per-row insert
const LOG_OHLCV_ROWS  = process.env.LOG_OHLCV_ROWS === '1';     // echo payload rows
const LOG_OHLCV_SQL   = process.env.LOG_OHLCV_SQL === '1';      // echo sql batch sizes
const VERIFY_OHLCV    = process.env.VERIFY_OHLCV === '1';       // SELECT verify after insert

// Helpers
function keyOf(pool_id, bucket_start) {
  return `${pool_id}__${new Date(bucket_start).toISOString()}`;
}

function aggregateBatch(items) {
  const map = new Map();
  for (const it of items) {
    const k = keyOf(it.pool_id, it.bucket_start);
    const prev = map.get(k);
    if (!prev) {
      map.set(k, {
        pool_id: it.pool_id,
        bucket_start: it.bucket_start,
        high: it.price,
        low:  it.price,
        close: it.price,
        volume_zig: it.vol_zig || 0,
        trade_count: it.trade_inc || 0,
        liquidity_zig: it.liquidity_zig ?? null,
      });
    } else {
      if (it.price > prev.high) prev.high = it.price;
      if (it.price < prev.low)  prev.low  = it.price;
      prev.close = it.price;
      prev.volume_zig += (it.vol_zig || 0);
      prev.trade_count += (it.trade_inc || 0);
      if (it.liquidity_zig != null) prev.liquidity_zig = it.liquidity_zig;
    }
  }
  return Array.from(map.values());
}

async function fetchPrevCloses(rows) {
  if (!rows.length) return new Map();

  const params = [];
  const valuesSQL = rows.map((r, idx) => {
    const i = idx * 2;
    params.push(r.pool_id, r.bucket_start);
    return `($${i + 1}::BIGINT, $${i + 2}::timestamptz)`;
  }).join(',');

  const sql = `
    WITH keys(pool_id, bucket_start) AS ( VALUES ${valuesSQL} )
    SELECT k.pool_id, k.bucket_start, o.close
    FROM keys k
    LEFT JOIN ohlcv_1m o
      ON o.pool_id = k.pool_id
     AND o.bucket_start = (k.bucket_start - INTERVAL '1 minute')
  `;

  const { rows: prevs } = await DB.query(sql, params);
  const out = new Map();
  for (const r of prevs) {
    const k = keyOf(r.pool_id, r.bucket_start);
    out.set(k, r.close == null ? null : Number(r.close));
  }
  return out;
}

function buildInsertSQL(rowsWithOpens) {
  const cols = [
    'pool_id','bucket_start','open','high','low','close',
    'volume_zig','trade_count','liquidity_zig'
  ];
  const placeholders = [];
  const args = [];
  let p = 1;

  for (const r of rowsWithOpens) {
    placeholders.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
    args.push(
      r.pool_id,
      r.bucket_start,
      r.open, r.high, r.low, r.close,
      r.volume_zig || 0,
      r.trade_count || 0,
      r.liquidity_zig ?? null
    );
  }

  const sql = `
    INSERT INTO ohlcv_1m
      (${cols.join(',')})
    VALUES
      ${placeholders.join(',')}
    ON CONFLICT (pool_id, bucket_start) DO UPDATE
      SET high          = GREATEST(ohlcv_1m.high, EXCLUDED.high),
          low           = LEAST(ohlcv_1m.low,  EXCLUDED.low),
          close         = EXCLUDED.close,
          volume_zig    = ohlcv_1m.volume_zig + EXCLUDED.volume_zig,
          trade_count   = ohlcv_1m.trade_count + EXCLUDED.trade_count,
          liquidity_zig = COALESCE(EXCLUDED.liquidity_zig, ohlcv_1m.liquidity_zig)
    RETURNING pool_id, bucket_start, open, high, low, close, volume_zig, trade_count
  `;
  return { sql, args };
}

async function verifyRow(pool_id, bucket_start) {
  if (!VERIFY_OHLCV) return;
  const { rows } = await DB.query(
    `SELECT 1 FROM ohlcv_1m WHERE pool_id=$1 AND bucket_start=$2 LIMIT 1`,
    [pool_id, bucket_start]
  );
  if (rows.length) {
    console.info('[ohlcv/verify] present', { pool: pool_id, bucket: bucket_start });
  } else {
    console.warn('[ohlcv/verify] NOT FOUND', { pool: pool_id, bucket: bucket_start });
  }
}

// Strict single-row path (great for debugging)
async function insertOneStrict(r) {
  const sql = `
    INSERT INTO ohlcv_1m
      (pool_id,bucket_start,open,high,low,close,volume_zig,trade_count,liquidity_zig)
    VALUES
      ($1,$2,$3,$4,$5,$6,$7,$8,$9)
    ON CONFLICT (pool_id, bucket_start) DO UPDATE
      SET high          = GREATEST(ohlcv_1m.high, EXCLUDED.high),
          low           = LEAST(ohlcv_1m.low,  EXCLUDED.low),
          close         = EXCLUDED.close,
          volume_zig    = ohlcv_1m.volume_zig + EXCLUDED.volume_zig,
          trade_count   = ohlcv_1m.trade_count + EXCLUDED.trade_count,
          liquidity_zig = COALESCE(EXCLUDED.liquidity_zig, ohlcv_1m.liquidity_zig)
    RETURNING pool_id, bucket_start
  `;
  const args = [
    r.pool_id, r.bucket_start, r.open, r.high, r.low, r.close,
    r.volume_zig || 0, r.trade_count || 0, r.liquidity_zig ?? null
  ];

  if (LOG_OHLCV_ROWS) console.debug('[ohlcv/strict] row', r);
  const res = await DB.query(sql, args);
  if (res.rowCount > 0) {
    console.info('[ohlcv/strict] UPSERT OK', res.rows[0]);
  }
  await verifyRow(r.pool_id, r.bucket_start);
}

const ohlcvQueue = new BatchQueue({
  maxItems: Number(process.env.OHLCV_BATCH_MAX || 600),
  maxWaitMs: Number(process.env.OHLCV_BATCH_WAIT_MS || 120),
  flushFn: async (items) => {
    if (!items.length) return;

    // 1) one row per (pool_id,bucket)
    const agg = aggregateBatch(items);

    // 2) prev closes
    const prevMap = await fetchPrevCloses(agg);

    // 3) choose open = prev close if exists, else first seen price (we stored in 'close' during agg)
    const rowsWithOpens = agg.map(r => {
      const prev = prevMap.get(keyOf(r.pool_id, r.bucket_start));
      const open = (prev ?? r.close);
      return { ...r, open };
    });

    if (STRICT_OHLCV) {
      for (const r of rowsWithOpens) await insertOneStrict(r);
      return;
    }

    // 4) single multi-row upsert
    const { sql, args } = buildInsertSQL(rowsWithOpens);
    if (LOG_OHLCV_SQL) console.debug('[ohlcv/sql] rows', rowsWithOpens.length, 'args', args.length);

    const res = await DB.query(sql, args);
    if (res?.rowCount > 0 && LOG_OHLCV_SQL) {
      const sample = res.rows.slice(0, Math.min(3, res.rowCount));
      for (const s of sample) console.debug('[ohlcv/upsert] sample', s);
    }

    if (VERIFY_OHLCV) {
      for (const r of rowsWithOpens) await verifyRow(r.pool_id, r.bucket_start);
    }
  }
});

// Public API
export async function upsertOHLCV1m({ pool_id, bucket_start, price, vol_zig, trade_inc, liquidity_zig = null }) {
  ohlcvQueue.push({
    pool_id,
    bucket_start,
    price,
    vol_zig: vol_zig || 0,
    trade_inc: trade_inc || 0,
    liquidity_zig
  });
}

export async function drainOHLCV() { await ohlcvQueue.drain(); }
