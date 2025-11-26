// common/db-clickhouse.js
import 'dotenv/config';
import { createClient } from '@clickhouse/client';
import { info, warn } from './log.js';

const CH_OPTS = {
  host: process.env.CLICKHOUSE_HOST || 'http://localhost:8123',
  username: process.env.CLICKHOUSE_USER || 'default',
  password: process.env.CLICKHOUSE_PASSWORD || '',
  database: process.env.CLICKHOUSE_DB || 'degenter',
  request_timeout: Number(process.env.CLICKHOUSE_TIMEOUT_MS || 60_000),
  compression: { response: true }
};

const CH = createClient(CH_OPTS);

export function chInfo() {
  return {
    host: CH_OPTS.host,
    database: CH_OPTS.database,
    username: CH_OPTS.username
  };
}

export async function chPing() {
  await CH.ping();
  info('[clickhouse] ping ok', chInfo());
}

export async function chInsertJSON({ table, rows, format = 'JSONEachRow' }) {
  if (!rows?.length) return;
  const started = Date.now();
  try {
    await CH.insert({
      table,
      values: rows,
      format, // JSONEachRow
    });
    info('[clickhouse] insert', { table, rows: rows.length, ms: Date.now() - started });
  } catch (e) {
    warn('[clickhouse] insert failed', { table, rows: rows.length, err: e?.message || e });
    throw e;
  }
}

export async function chQuery({ query, params = {} }) {
  const rs = await CH.query({ query, params, format: 'JSONEachRow' });
  return await rs.json();
}

export async function chClose() {
  try { await CH.close(); info('[clickhouse] closed'); }
  catch (e) { warn('[clickhouse] close error', e?.message || e); }
}

export default { chPing, chInsertJSON, chQuery, chClose, CH };
