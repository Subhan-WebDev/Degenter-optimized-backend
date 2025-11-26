import 'dotenv/config';
import '../../common/load-env.js';  // must be first import
process.env.SVC_NAME = process.env.SVC_NAME || 'ingester-backfill';

import { createRedisClient } from '../../common/redis-client.js';
import { getStatus, getBlock, getBlockResults, unwrapStatus, unwrapBlock, unwrapBlockResults } from '../../common/rpc.js';
import { getIndexHeight, bumpIndexHeight } from '../../common/index_state.js';
import { info, warn, err, debug } from '../../common/log.js';

const STREAM = process.env.STREAM_RAW || 'chain:raw_blocks';
const CURSOR_ID = process.env.INDEX_CURSOR_ID || 'core';

const BATCH = Number(process.env.BACKFILL_BATCH || 40);
const SLEEP = Number(process.env.BACKFILL_SLEEP_MS || 60);
const TIP_NEAR = Number(process.env.BACKFILL_TIP_NEAR || 2); // when <= this gap, we consider "at tip"

const { client: redis, connect: redisConnect } = createRedisClient('backfill');

async function xaddRawBlock(payload) {
  return redis.xAdd(STREAM, '*', { j: JSON.stringify(payload) });
}

async function isWssUp() {
  try { return (await redis.get('ctrl:ingest:wss_status')) === 'up'; }
  catch { return false; }
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function main() {
  await redisConnect();
  info('[backfill] connected to redis');

  // resume from index_state
  let cursor = Number(await getIndexHeight(CURSOR_ID) || 0);
  if (cursor > 0) info('[backfill] resume from index_state', cursor);
  else info('[backfill] starting with cursor=0 (will catch latest and then handoff)');

  // loop forever as fallback and gap-filler
  for (;;) {
    try {
      const st = await getStatus();
      const tip = unwrapStatus(st);
      if (!tip) { await sleep(1000); continue; }

      // if websocket is up and we're at tip — nap
      const wup = await isWssUp();
      const gap = Math.max(0, tip - cursor);
      if (wup && gap <= TIP_NEAR) {
        debug('[backfill] at tip; ws up → idle', { cursor, tip });
        await sleep(1500);
        continue;
      }

      // fetch a window towards tip
      const from = cursor + 1;
      const to = Math.min(cursor + BATCH, tip);
      if (to < from) { await sleep(500); continue; }

      info(`[backfill] heights ${from}..${to} (tip=${tip}, ws=${wup ? 'up' : 'down'})`);

      for (let h = from; h <= to; h++) {
        try {
          const [b, r] = await Promise.all([ getBlock(h), getBlockResults(h) ]);
          const blk = unwrapBlock(b);
          const res = unwrapBlockResults(r);
          if (!blk?.header) { warn('[backfill] no header at', h); continue; }

          const payload = {
            height: Number(blk.header.height),
            time: blk.header.time,
            header: blk.header,
            txs: blk.txs,
            results: res.txs_results
          };

          await xaddRawBlock(payload);
          await bumpIndexHeight(CURSOR_ID, payload.height);
          cursor = payload.height;
        } catch (e) {
          warn('[backfill] height error', h, e?.message || e);
        }
        if (SLEEP) await sleep(SLEEP);
      }

      // once we newly hit tip (or near), hint realtime to lead
      if (tip - cursor <= TIP_NEAR) {
        try { await redis.set('ctrl:ingest:last_at_tip', String(Date.now())); } catch {}
      }
    } catch (e) {
      warn('[backfill] loop', e?.message || e);
      await sleep(1000);
    }
  }
}

main().catch(e => { err(e); process.exit(1); });
