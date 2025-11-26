// common/db-timescale.js
// import 'dotenv/config';
import { Pool } from 'pg';
import { info, warn } from './log.js';
// common/db-timescale.js
import dotenv from 'dotenv';
import path from 'node:path';
import fs from 'node:fs';

const candidates = [
  process.env.DOTENV_CONFIG_PATH,
  path.resolve(process.cwd(), '.env'),
  path.resolve(process.cwd(), '../../.env'),
];

for (const p of candidates) {
  if (p && fs.existsSync(p)) { dotenv.config({ path: p }); break; }
}


const DEFAULTS = {
  max: Number(process.env.PG_POOL_MAX || 16),
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 10_000,
};

export const TS = new Pool({
  connectionString: process.env.TIMESCALE_DATABASE_URL || process.env.DATABASE_URL,
  ...DEFAULTS,
});

TS.on('connect', (client) => {
  client.query(`
    SET application_name = '${process.env.SVC_NAME || 'degenter-v2'}';
    SET statement_timeout = '120s';
    SET idle_in_transaction_session_timeout = '60s';
  `).catch(()=>{});
});

export async function tsInit() {
  const r = await TS.query('SELECT NOW() as now');
  info('[timescale] connected @', r.rows[0].now);
}

export async function tsTx(fn) {
  const client = await TS.connect();
  try {
    await client.query('BEGIN');
    const res = await fn(client);
    await client.query('COMMIT');
    return res;
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

export async function tsQueryRetry(sql, args = [], attempts = 3) {
  for (let i = 0; i < attempts; i++) {
    try { return await TS.query(sql, args); }
    catch (e) {
      if (i === attempts - 1) throw e;
      warn('[timescale] retry', i + 1, e.message);
      await new Promise(r => setTimeout(r, 150 * (i + 1)));
    }
  }
}

export async function tsClose() {
  try { await TS.end(); info('[timescale] pool closed'); }
  catch (e) { warn('[timescale] close error:', e.message); }
}

/* ── Back-compat aliases (so old code keeps working) ───────────────────── */
export const DB = TS;
export const init = tsInit;
export const tx = tsTx;
export const queryRetry = tsQueryRetry;
export const close = tsClose;

export default { TS, tsInit, tsTx, tsQueryRetry, tsClose, DB, init, tx, queryRetry, close };
