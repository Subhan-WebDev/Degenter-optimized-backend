
// services/worker-clickhouse/writers/pool_state.js
import { chInsertJSON } from '../../../common/db-clickhouse.js';

const buffer = [];
const MAX_BUFFER = Number(process.env.CLICKHOUSE_POOLSTATE_BUFFER || 500);
const FLUSH_MS   = Number(process.env.CLICKHOUSE_POOLSTATE_FLUSH_MS || 2000);

export function pushPoolState(row) {
  buffer.push(row);
  if (buffer.length >= MAX_BUFFER) {
    flushPoolState().catch(()=>{});
  }
}

export async function flushPoolState() {
  if (!buffer.length) return;
  const batch = buffer.splice(0, buffer.length);
  await chInsertJSON({ table: 'pool_state', rows: batch });
}

setInterval(() => { flushPoolState().catch(()=>{}); }, FLUSH_MS);
