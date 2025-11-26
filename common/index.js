// common/index_state.js
import { TS as DB } from './db-timescale.js';
import { warn } from './log.js';

const TABLE_SQL = `
  CREATE TABLE IF NOT EXISTS public.index_state (
    id TEXT PRIMARY KEY,
    last_height BIGINT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
  );
`;

async function ensure() {
  try { await DB.query(TABLE_SQL); } catch (e) { warn('[index_state.ensure]', e.message || e); }
}

export async function getIndexHeight(id = 'core') {
  await ensure();
  const { rows } = await DB.query(`SELECT last_height FROM public.index_state WHERE id=$1`, [id]);
  return Number(rows?.[0]?.last_height || 0);
}

export async function setIndexHeight(id, height) {
  await ensure();
  await DB.query(`
    INSERT INTO public.index_state(id, last_height, updated_at)
    VALUES ($1, $2, now())
    ON CONFLICT (id) DO UPDATE
      SET last_height = EXCLUDED.last_height,
          updated_at  = now()
  `, [id, Number(height || 0)]);
}

export async function bumpIndexHeight(id, height) {
  await ensure();
  await DB.query(`
    INSERT INTO public.index_state(id, last_height, updated_at)
    VALUES ($1, $2, now())
    ON CONFLICT (id) DO UPDATE
      SET last_height = GREATEST(public.index_state.last_height, EXCLUDED.last_height),
          updated_at  = now()
  `, [id, Number(height || 0)]);
}
