import 'dotenv/config';
import '../../common/load-env.js';
process.env.SVC_NAME = process.env.SVC_NAME || 'ingester-realtime';

import WebSocket from 'ws';
import { createRedisClient } from '../../common/redis-client.js';
import { getStatus, getBlock, getBlockResults, unwrapStatus, unwrapBlock, unwrapBlockResults } from '../../common/rpc.js';
import { getIndexHeight, bumpIndexHeight } from '../../common/index_state.js';
import { info, warn, err, debug } from '../../common/log.js';

const STREAM = process.env.STREAM_RAW || 'chain:raw_blocks';
const CURSOR_ID = process.env.INDEX_CURSOR_ID || 'core';
const WS_URL = (process.env.WS_URL || '').trim();     // e.g. wss://rpc.example.com/websocket
const POLL_MS = Number(process.env.REALTIME_POLL_MS || 1200); // for HTTP fallback

const { client: redis, connect: redisConnect } = createRedisClient('realtime');

async function setWsStatus(up) {
  try { await redis.set('ctrl:ingest:wss_status', up ? 'up' : 'down'); }
  catch {}
}
async function xaddRawBlock(payload) {
  return redis.xAdd(STREAM, '*', { j: JSON.stringify(payload) });
}

async function catchUpGaps(fromHeightExclusive, toHeightInclusive) {
  const start = Number(fromHeightExclusive || 0) + 1;
  const end = Number(toHeightInclusive || 0);
  if (!end || end < start) return;

  info('[realtime] catching up', { start, end });
  for (let h = start; h <= end; h++) {
    try {
      const [b, r] = await Promise.all([ getBlock(h), getBlockResults(h) ]);
      const blk = unwrapBlock(b);
      const res = unwrapBlockResults(r);
      if (!blk?.header) { warn('[realtime/catchup] missing header at', h); continue; }
      const payload = {
        height: Number(blk.header.height),
        time: blk.header.time,
        header: blk.header,
        txs: blk.txs,
        results: res.txs_results
      };
      await xaddRawBlock(payload);
      await bumpIndexHeight(CURSOR_ID, payload.height);
    } catch (e) {
      warn('[realtime/catchup]', h, e?.message || e);
    }
  }
}

async function runWebSocket() {
  if (!WS_URL) return false;

  await setWsStatus(false);
  let cursor = await getIndexHeight(CURSOR_ID);
  info('[realtime/ws] starting; cursor=', cursor, 'url=', WS_URL);

  const ws = new WebSocket(WS_URL);
  let open = false;
  let subSent = false;

  ws.on('open', async () => {
    open = true;
    info('[realtime/ws] open');
    try {
      // Tendermint subscribe
      ws.send(JSON.stringify({ jsonrpc: '2.0', method: 'subscribe', id: '1', params: { query: "tm.event='NewBlock'" } }));
      subSent = true;
      await setWsStatus(true);
    } catch (e) { warn('[realtime/ws] subscribe', e?.message || e); }
  });

  ws.on('message', async (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      // We only care about NewBlock notifications with block header
      const header = msg?.result?.data?.value?.block?.header;
      const h = Number(header?.height || 0);
      if (!h) return;

      // fix gaps (if any)
      const last = Number(await getIndexHeight(CURSOR_ID) || 0);
      if (h > last + 1) {
        await catchUpGaps(last, h - 1);
      }

      // get block_results for events/codes
      const r = await getBlockResults(h);
      const res = unwrapBlockResults(r);

      const payload = {
        height: h,
        time: header.time,
        header,
        txs: msg?.result?.data?.value?.block?.data?.txs || [],
        results: res.txs_results
      };

      await xaddRawBlock(payload);
      await bumpIndexHeight(CURSOR_ID, h);
      debug('[realtime/ws] XADD height', h);
    } catch (e) {
      warn('[realtime/ws] message err', e?.message || e);
    }
  });

  const onCloseOrError = async (label, e) => {
    warn(`[realtime/ws] ${label}`, e?.message || e || '');
    open = false;
    try { ws.close(); } catch {}
    await setWsStatus(false);
  };

  ws.on('close', (c) => onCloseOrError('close ' + c));
  ws.on('error', (e) => onCloseOrError('error', e));

  // keep the process alive while socket is open
  for (;;) {
    if (!open) break;
    await new Promise(r => setTimeout(r, 500));
  }
  return subSent; // we did try WS; allow caller to retry
}

async function runHttpPolling() {
  await setWsStatus(false);
  let lastSeen = await getIndexHeight(CURSOR_ID);

  info('[realtime/http] fallback polling; resume cursor=', lastSeen);
  // soft resume from stream tail (optional)
  try {
    const tail = await redis.xRevRange(STREAM, '+', '-', { COUNT: 1 });
    if (tail?.length) {
      const last = tail[0][1].j && JSON.parse(tail[0][1].j);
      lastSeen = Math.max(lastSeen, Number(last?.header?.height || last?.height || 0));
      info('[realtime/http] resume from stream tail height', lastSeen);
    }
  } catch (e) {
    warn('[realtime/http] resume scan failed', e?.message || e);
  }

  for (;;) {
    try {
      const st = await getStatus();
      const tip = unwrapStatus(st);
      if (!tip) { await new Promise(r => setTimeout(r, POLL_MS)); continue; }

      const start = Math.max(lastSeen, tip - 5);
      for (let h = start + 1; h <= tip; h++) {
        const [b, r] = await Promise.all([ getBlock(h), getBlockResults(h) ]);
        const blk = unwrapBlock(b);
        const res = unwrapBlockResults(r);
        if (!blk?.header) { warn('[realtime/http] no header at', h); continue; }

        const payload = {
          height: Number(blk.header.height),
          time: blk.header.time,
          header: blk.header,
          txs: blk.txs,
          results: res.txs_results
        };

        await xaddRawBlock(payload);
        await bumpIndexHeight(CURSOR_ID, payload.height);
        lastSeen = payload.height;
        info('[realtime/http] XADD', STREAM, 'height', lastSeen);
      }
    } catch (e) {
      warn('[realtime/http] loop', e?.message || e);
    }
    await new Promise(r => setTimeout(r, POLL_MS));
  }
}

async function main() {
  await redisConnect();
  info('[realtime] connected to redis');

  // Outer loop: prefer WS; on disconnect, rely on backfill to keep up, and retry WS periodically.
  for (;;) {
    const tried = await runWebSocket();
    if (!tried) {
      warn('[realtime] WS_URL not set; using HTTP polling mode');
      await runHttpPolling(); // never returns
      return;
    }
    // Short delay before retrying WS
    await new Promise(r => setTimeout(r, 1500));
  }
}

main().catch(e => { err(e); process.exit(1); });
