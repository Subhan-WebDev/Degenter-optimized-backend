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

// Format to ClickHouse-friendly DateTime64: 'YYYY-MM-DD HH:MM:SS.mmm' (UTC)
export function toChDateTime(v) {
  const d = v instanceof Date ? v : new Date(v);
  const safe = isNaN(d.getTime()) ? new Date() : d;
  return safe.toISOString().slice(0, -1).replace('T', ' ');
}

function sanitizeValue(v) {
  if (v instanceof Date) return toChDateTime(v);
  if (Array.isArray(v)) return v.map((x) => sanitizeValue(x));
  if (v && typeof v === 'object') {
    return Object.fromEntries(Object.entries(v).map(([k, val]) => [k, sanitizeValue(val)]));
  }
  return v;
}

function normalizeRows(rows) {
  if (!rows) return [];
  if (typeof rows === 'string') {
    return rows
      .trim()
      .split('\n')
      .filter(Boolean)
      .map((line) => sanitizeValue(JSON.parse(line)));
  }
  if (Array.isArray(rows)) return rows.map((r) => sanitizeValue(r));
  if (typeof rows === 'object') return [sanitizeValue(rows)];
  return [];
}

export async function chInsertJSON({ table, rows, format = 'JSONEachRow' }) {
  const normalized = normalizeRows(rows);
  if (!normalized.length) return;
  const started = Date.now();
  try {
    await CH.insert({
      table,
      values: normalized,
      format, // JSONEachRow
    });
    info('[clickhouse] insert', { table, rows: normalized.length, ms: Date.now() - started });
  } catch (e) {
    warn('[clickhouse] insert failed', { table, rows: normalized.length, err: e?.message || e });
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
