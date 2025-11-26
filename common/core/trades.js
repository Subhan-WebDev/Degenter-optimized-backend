// common/core/trades.js
import { DB } from '../../common/db-timescale.js';
import BatchQueue from '../batch.js';

const CONFLICT_TARGET =
  '(tx_hash, pool_id, msg_index, event_index, created_at)';

const INSERT_SQL = `
  INSERT INTO trades
   (pool_id, pair_contract, action, direction,
    offer_asset_denom, offer_amount_base,
    ask_asset_denom, ask_amount_base,
    return_amount_base, is_router,
    reserve_asset1_denom, reserve_asset1_amount_base,
    reserve_asset2_denom, reserve_asset2_amount_base,
    height, tx_hash, signer, msg_index, event_index, created_at)
  VALUES %VALUES%
  ON CONFLICT ${CONFLICT_TARGET} DO NOTHING
  RETURNING trade_id, tx_hash, pool_id, msg_index, event_index, created_at
`;

// env toggles
const STRICT_TRADES    = process.env.STRICT_TRADES === '1';     // per-row insert
const LOG_TRADE_ROWS   = process.env.LOG_TRADE_ROWS === '1';    // echo payload rows
const LOG_TRADE_SQL    = process.env.LOG_TRADE_SQL === '1';     // echo sql batch sizes
const VERIFY_AFTER_INS = process.env.VERIFY_AFTER_INS === '1';  // quick SELECT 1 after insert

function sqlValues(rows) {
  const vals = [];
  const args = [];
  let i = 1;
  for (const t of rows) {
    vals.push(`(
      $${i++},$${i++},$${i++},$${i++},
      $${i++},$${i++},
      $${i++},$${i++},
      $${i++},$${i++},
      $${i++},$${i++},
      $${i++},$${i++},
      $${i++},$${i++},$${i++},$${i++},$${i++},$${i++}
    )`);
    args.push(
      t.pool_id, t.pair_contract, t.action, t.direction,
      t.offer_asset_denom, t.offer_amount_base,
      t.ask_asset_denom, t.ask_amount_base,
      t.return_amount_base, t.is_router,
      t.reserve_asset1_denom, t.reserve_asset1_amount_base,
      t.reserve_asset2_denom, t.reserve_asset2_amount_base,
      t.height, t.tx_hash, t.signer,
      Number(t.msg_index ?? 0),
      Number(t.event_index ?? 0),
      t.created_at
    );
  }
  return { text: INSERT_SQL.replace('%VALUES%', vals.join(',')), args };
}

async function verifyRowExists(t) {
  const q = `
    SELECT 1 FROM trades
    WHERE tx_hash=$1 AND pool_id=$2 AND msg_index=$3 AND event_index=$4 AND created_at=$5
    LIMIT 1
  `;
  const { rows } = await DB.query(q, [
    t.tx_hash, t.pool_id, Number(t.msg_index ?? 0), Number(t.event_index ?? 0), t.created_at
  ]);
  if (rows.length) {
    console.info('[trades/verify] present', {
      tx: t.tx_hash, pool: t.pool_id, msg: t.msg_index, evt: t.event_index, at: t.created_at
    });
  } else {
    console.warn('[trades/verify] NOT FOUND AFTER INSERT', {
      tx: t.tx_hash, pool: t.pool_id, msg: t.msg_index, evt: t.event_index, at: t.created_at
    });
  }
}

// Strict single-row insert (great for debugging)
async function insertOneStrict(t) {
  const text = `
    INSERT INTO trades
     (pool_id, pair_contract, action, direction,
      offer_asset_denom, offer_amount_base,
      ask_asset_denom, ask_amount_base,
      return_amount_base, is_router,
      reserve_asset1_denom, reserve_asset1_amount_base,
      reserve_asset2_denom, reserve_asset2_amount_base,
      height, tx_hash, signer, msg_index, event_index, created_at)
    VALUES
     ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
    ON CONFLICT ${CONFLICT_TARGET} DO NOTHING
    RETURNING trade_id
  `;
  const args = [
    t.pool_id, t.pair_contract, t.action, t.direction,
    t.offer_asset_denom, t.offer_amount_base,
    t.ask_asset_denom, t.ask_amount_base,
    t.return_amount_base, t.is_router,
    t.reserve_asset1_denom, t.reserve_asset1_amount_base,
    t.reserve_asset2_denom, t.reserve_asset2_amount_base,
    t.height, t.tx_hash, t.signer,
    Number(t.msg_index ?? 0),
    Number(t.event_index ?? 0),
    t.created_at
  ];

  try {
    if (LOG_TRADE_ROWS) console.debug('[trades/strict] row', t);
    const res = await DB.query(text, args);
    if (res.rowCount === 0) {
      console.warn('[trades/strict] CONFLICT skip', {
        tx: t.tx_hash, pool: t.pool_id, msg: t.msg_index, evt: t.event_index, at: t.created_at
      });
    } else {
      console.info('[trades/strict] INSERT OK', {
        trade_id: res.rows?.[0]?.trade_id,
        tx: t.tx_hash, pool: t.pool_id, msg: t.msg_index, evt: t.event_index, at: t.created_at
      });
    }
    if (VERIFY_AFTER_INS) await verifyRowExists(t);
  } catch (e) {
    console.error('[trades/strict] INSERT ERROR', e?.message || e, { row: t });
    throw e;
  }
}

const tradesQueue = new BatchQueue({
  maxItems: Number(process.env.TRADES_BATCH_MAX || 800),
  maxWaitMs: Number(process.env.TRADES_BATCH_WAIT_MS || 120),
  flushFn: async (items) => {
    if (!items.length) return;

    if (STRICT_TRADES) {
      for (const t of items) await insertOneStrict(t);
      return;
    }

    if (LOG_TRADE_ROWS) {
      for (const t of items) console.debug('[trades/row]', t);
    }

    const { text, args } = sqlValues(items);
    try {
      if (LOG_TRADE_SQL) {
        console.debug('[trades/sql] batch size', items.length, 'args', args.length);
      }
      const res = await DB.query(text, args);
      const inserted = res?.rowCount || 0;
      const attempted = items.length;

      if (inserted > 0) {
        console.info('[trades/flush] INSERTED', { attempted, inserted });
        const sample = (res.rows || []).slice(0, Math.min(5, res.rowCount));
        for (const r of sample) {
          console.info('[trades/flush] INSERT OK sample', {
            trade_id: r.trade_id,
            tx: r.tx_hash, pool: r.pool_id, msg: r.msg_index, evt: r.event_index, at: r.created_at
          });
        }
      }

      if (inserted !== attempted) {
        const key = (t) => `${t.tx_hash}|${t.pool_id}|${Number(t.msg_index ?? 0)}|${Number(t.event_index ?? 0)}|${new Date(t.created_at).toISOString()}`;
        const attemptedKeys = new Set(items.map(key));

        for (const r of res.rows || []) {
          attemptedKeys.delete(`${r.tx_hash}|${r.pool_id}|${Number(r.msg_index)}|${Number(r.event_index)}|${new Date(r.created_at).toISOString()}`);
        }

        const conflicts = Array.from(attemptedKeys.values());
        console.warn('[trades/flush] conflicts', {
          attempted, inserted, conflicts_count: conflicts.length
        });

        const sample = items.filter(t => conflicts.includes(key(t))).slice(0, 5);
        for (const s of sample) {
          console.warn('[trades/conflict] skipped', {
            tx: s.tx_hash, pool: s.pool_id, msg: s.msg_index, evt: s.event_index, at: s.created_at
          });
        }
      }
    } catch (e) {
      console.error('[trades/flush] INSERT ERROR', e?.message || e);
      console.error('[trades/flush] sample row', items[0]);
      throw e;
    }
  }
});

export async function insertTrade(t) {
  tradesQueue.push(t);
}

export async function drainTrades() {
  await tradesQueue.drain();
}
