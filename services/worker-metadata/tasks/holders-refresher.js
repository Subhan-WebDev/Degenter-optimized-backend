import { TS as DB } from '../../../common/db-timescale.js';
import { lcdDenomOwners } from '../../../common/lcd.js';
import { info, warn } from '../../../common/log.js';

const HOLDERS_REFRESH_SEC = parseInt(process.env.HOLDERS_REFRESH_SEC || '180', 10);
const HOLDERS_BATCH_SIZE = parseInt(process.env.HOLDERS_BATCH_SIZE || '4', 10);
const MAX_HOLDER_PAGES_PER_CYCLE = parseInt(process.env.MAX_HOLDER_PAGES_PER_CYCLE || '30', 10);
const LCD_PAGE_CONCURRENCY = parseInt(process.env.LCD_PAGE_CONCURRENCY || '4', 10);

function digitsOrNull(x){ const s=String(x??''); return /^\d+$/.test(s)?s:null; }
const isIbcDenom = d => typeof d === 'string' && d.startsWith('ibc/');

async function bumpStatsTimestampOnly(token_id) {
  await DB.query(`
    INSERT INTO token_holders_stats(token_id, holders_count, updated_at)
    VALUES ($1, NULL, now())
    ON CONFLICT (token_id) DO UPDATE
      SET updated_at = now()
  `, [token_id]);
}

class Semaphore {
  constructor(n){ this.n=n; this.q=[]; }
  async acquire(){ if(this.n>0){ this.n--; return; } await new Promise(res=>this.q.push(res)); }
  release(){ const next=this.q.shift(); if(next) next(); else this.n++; }
}
const pageSem = new Semaphore(LCD_PAGE_CONCURRENCY);

async function fetchOwnersPageThrottled(denom, nextKey) {
  await pageSem.acquire();
  try { return await lcdDenomOwners(denom, nextKey); }
  finally { pageSem.release(); }
}

export async function refreshHoldersOnce(token_id, denom, maxPages = MAX_HOLDER_PAGES_PER_CYCLE) {
  if (!token_id || !denom) return;
  if (isIbcDenom(denom)) { info('[holders/once] skip IBC', denom); await bumpStatsTimestampOnly(token_id); return; }

  const seen = new Set();
  let nextKey = null;

  for (let i = 0; i < maxPages; i++) {
    let page;
    try { page = await fetchOwnersPageThrottled(denom, nextKey); }
    catch (e) {
      const msg = String(e?.message || '');
      if (msg.includes('501')) { warn('[holders/owners 501]', denom, 'skipping'); await bumpStatsTimestampOnly(token_id); return; }
      warn('[holders/owners]', denom, msg);
      break;
    }

    const items = page?.denom_owners || [];
    const client = await DB.connect();
    try {
      await client.query('BEGIN');
      for (const it of items) {
        const addr = it.address;
        const amt = it.balance?.amount || '0';
        seen.add(addr);
        await client.query(`
          INSERT INTO holders(token_id, address, balance_base, updated_at)
          VALUES ($1,$2,$3, now())
          ON CONFLICT (token_id, address) DO UPDATE SET
            balance_base = EXCLUDED.balance_base,
            updated_at   = now()
        `, [token_id, addr, digitsOrNull(amt)]);
        await client.query(`
          INSERT INTO wallets(address, last_seen)
          VALUES ($1, now())
          ON CONFLICT (address) DO NOTHING
        `, [addr]);
      }
      await client.query('COMMIT');
    } catch (e) { await client.query('ROLLBACK'); throw e; }
    finally { client.release(); }

    nextKey = page?.pagination?.next_key || null;
    if (!nextKey) break;
  }

  const all = Array.from(seen);
  const client = await DB.connect();
  try {
    await client.query('BEGIN');
    if (all.length > 0) {
      const params = [token_id, ...all];
      const placeholders = all.map((_, i) => `$${i + 2}`).join(',');
      await client.query(`
        UPDATE holders
        SET balance_base = '0', updated_at = now()
        WHERE token_id = $1 AND address NOT IN (${placeholders})
      `, params);
    } else {
      await client.query(`
        UPDATE holders
        SET balance_base = '0', updated_at = now()
        WHERE token_id = $1
      `, [token_id]);
    }
    const { rows: hc } = await client.query(
      `SELECT COUNT(*)::BIGINT AS c FROM holders WHERE token_id=$1 AND balance_base::NUMERIC > 0`,
      [token_id]
    );
    await client.query(`
      INSERT INTO token_holders_stats(token_id, holders_count, updated_at)
      VALUES ($1, $2, now())
      ON CONFLICT (token_id) DO UPDATE
        SET holders_count = EXCLUDED.holders_count,
            updated_at    = now()
    `, [token_id, hc[0].c]);
    await client.query('COMMIT');
    info('[holders/once] updated', denom, 'count=', hc[0].c);
  } catch (e) { await client.query('ROLLBACK'); warn('[holders/once]', denom, e.message); }
  finally { client.release(); }
}

export function startHoldersRefresher() {
  (async function loop() {
    while (true) {
      try {
        const { rows } = await DB.query(`
          WITH cand AS (
            SELECT t.token_id, t.denom,
                   COALESCE(s.updated_at, TIMESTAMPTZ 'epoch') AS last_h_upd
            FROM tokens t
            LEFT JOIN token_holders_stats s ON s.token_id = t.token_id
            WHERE t.denom <> 'uzig' AND t.denom NOT LIKE 'ibc/%'
          )
          SELECT token_id, denom
          FROM cand
          ORDER BY last_h_upd ASC
          LIMIT $1
        `, [HOLDERS_BATCH_SIZE]);

        if (rows.length > 0) {
          await Promise.allSettled(rows.map(({ token_id, denom }) => refreshHoldersOnce(token_id, denom)));
        }
      } catch (e) { warn('[holders]', e.message); }
      await new Promise(r => setTimeout(r, HOLDERS_REFRESH_SEC * 1000));
    }
  })().catch(() => {});
}
