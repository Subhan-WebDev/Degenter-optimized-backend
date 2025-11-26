// common/db-clickhouse.js
import 'dotenv/config';
import { createClient } from '@clickhouse/client';
import { info, warn } from './log.js';

const CH = createClient({
  host: process.env.CLICKHOUSE_HOST || 'http://localhost:8123',
  username: process.env.CLICKHOUSE_USER || 'default',
  password: process.env.CLICKHOUSE_PASSWORD || '',
  database: process.env.CLICKHOUSE_DB || 'degenter',
  request_timeout: Number(process.env.CLICKHOUSE_TIMEOUT_MS || 60_000),
  compression: { response: true },
});

export async function chPing() {
  await CH.ping();
  info('[clickhouse] ping ok');
}

export async function chInsertJSON({ table, rows, format = 'JSONEachRow' }) {
  if (!rows?.length) return;
  await CH.insert({
    table,
    values: rows,
    format, // JSONEachRow
  });
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
