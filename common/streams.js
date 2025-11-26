// common/streams.js
import { setTimeout as sleep } from 'node:timers/promises';
import { info, warn, debug } from './log.js';

/**
 * createConsumerGroup({ redis, stream, group })
 */
export async function createConsumerGroup({ redis, stream, group }) {
  try {
    await redis.xGroupCreate(stream, group, '$', { MKSTREAM: true });
    info('[xgroup] created', { stream, group });
  } catch (e) {
    if (!String(e?.message || e).includes('BUSYGROUP')) throw e;
    info('[xgroup] exists', { stream, group });
  }
}

/**
 * claimStale({
 *   redis, stream, group, consumer,
 *   minIdleMs=60000, count=100
 * })
 * Returns array of IDs claimed to this consumer.
 */
export async function claimStale({
  redis, stream, group, consumer,
  minIdleMs = 60_000, count = 100
}) {
  try {
    const pending = await redis.xPending(stream, group);
    if (!pending || !pending.count) return [];
    const list = await redis.xPendingRange(
      stream, group, '-', '+', Math.min(count, pending.count), { idle: minIdleMs }
    );
    const staleIds = (list || []).map(x => x.id);
    if (!staleIds.length) return [];
    const claimed = await redis.xClaim(stream, group, consumer, minIdleMs, staleIds);
    debug('[xclaim] claimed', { stream, group, consumer, count: claimed?.length || 0 });
    return staleIds;
  } catch (e) {
    warn('[xclaim] error', e?.message || e);
    return [];
  }
}

/**
 * readLoop({
 *   redis, stream, group, consumer,
 *   handler,                       // async (records, { ackMany }) => void
 *   batch=100,                     // target batch size
 *   blockMs=5000,                  // first read can block up to this
 *   autoAck=false
 * })
 *
 * Improvements:
 *  - First read uses BLOCK=blockMs.
 *  - Then we "burst top-up" (no BLOCK) for up to STREAM_BURST_MS to reach `batch`.
 *  - Single ack via ackMany(ids) after handler finishes.
 *  - Optional periodic stale-claim (PEL recovery).
 */
export async function readLoop({
  redis, stream, group, consumer,
  handler,
  batch = 100,
  blockMs = 5_000,
  autoAck = false,
}) {
  const BURST_MS        = Number(process.env.STREAM_BURST_MS || 120);    // top-up window
  const CLAIM_IDLE_MS   = Number(process.env.STREAM_CLAIM_IDLE_MS || 60_000);
  const CLAIM_COUNT     = Number(process.env.STREAM_CLAIM_COUNT || 200);
  const CLAIM_EVERY     = Number(process.env.STREAM_CLAIM_EVERY || 25);  // cycles
  const BACKOFF_ON_ERR  = Number(process.env.STREAM_ERR_BACKOFF_MS || 200);

  info('[xread] starting', { stream, group, consumer, batch, blockMs, BURST_MS });

  let cycles = 0;

  // helper: normalize node-redis xReadGroup result -> [{id, map}, ...]
  const normalize = (res) => {
    if (!res || !Array.isArray(res) || !res.length) return [];
    // res = [{ name: stream, messages: [{ id, message: {k:v} }, ...] }]
    const messages = res[0]?.messages || [];
    return messages.map(m => ({
      id: m.id,
      map: Object.fromEntries(m.message ? Object.entries(m.message) : []),
    }));
  };

  // single read (optionally blocking)
  async function readOnce(count, useBlock) {
    const opts = { COUNT: count };
    if (useBlock && blockMs > 0) opts.BLOCK = blockMs;
    // id '>' => new messages
    const res = await redis.xReadGroup(group, consumer, { key: stream, id: '>' }, opts);
    return normalize(res);
  }

  async function ackMany(ids) {
    if (!ids?.length) return;
    try {
      await redis.xAck(stream, group, ids);
    } catch (e) {
      warn('[xack] error', e?.message || e, { count: ids.length });
    }
  }

  // main loop
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      cycles += 1;

      // 1) initial blocking read to get *some* messages
      let records = await readOnce(batch, true);

      // 2) burst top-up without BLOCK for a short time budget
      if (records.length < batch && BURST_MS > 0) {
        const start = Date.now();
        while (records.length < batch && (Date.now() - start) < BURST_MS) {
          const need = batch - records.length;
          if (need <= 0) break;
          const more = await readOnce(need, false);
          if (!more.length) break;
          records.push(...more);
        }
      }

      if (!records.length) {
        // periodically attempt to claim stale messages
        if (CLAIM_EVERY > 0 && cycles % CLAIM_EVERY === 0) {
          await claimStale({
            redis, stream, group, consumer,
            minIdleMs: CLAIM_IDLE_MS, count: CLAIM_COUNT
          });
        }
        continue;
      }

      debug('[xread] batch', stream, records.length);

      // 3) hand off to handler, collect ids to ack
      const idsToAck = [];
      try {
        await handler(records, {
          ack: async (id) => { if (id) idsToAck.push(id); },
          ackMany: async (ids) => { if (ids?.length) idsToAck.push(...ids); },
        });
      } catch (e) {
        warn('[xread] handler error', e?.message || e);
      }

      // 4) autoAck support (acks all read records)
      if (autoAck) {
        const allIds = records.map(r => r.id);
        await ackMany(allIds);
      } else if (idsToAck.length) {
        await ackMany(idsToAck);
      }

      // 5) periodic stale-claim
      if (CLAIM_EVERY > 0 && cycles % CLAIM_EVERY === 0) {
        await claimStale({
          redis, stream, group, consumer,
          minIdleMs: CLAIM_IDLE_MS, count: CLAIM_COUNT
        });
      }
    } catch (e) {
      warn('[xread] error', e?.message || e);
      await sleep(BACKOFF_ON_ERR);
    }
  }
}
