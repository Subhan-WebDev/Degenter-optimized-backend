// common/redis-client.js
import { createClient } from 'redis';
import { info, warn, err } from './log.js';

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const TLS = process.env.REDIS_TLS === '1' || REDIS_URL.startsWith('rediss://');

export function createRedisClient(name = 'default') {
  const client = createClient({
    url: REDIS_URL,
    socket: TLS ? { tls: true } : undefined,
  });

  client.on('error', (e) => err(`[redis:${name}]`, e?.message || e));
  client.on('reconnecting', () => warn(`[redis:${name}] reconnecting…`));
  client.on('ready', () => info(`[redis:${name}] ready`));
  client.on('connect', () => info(`[redis:${name}] connect`));
  client.on('end', () => warn(`[redis:${name}] end`));

  const connect = async () => {
    if (!client.isOpen) await client.connect();
    return client;
  };
  const ping = async () => client.ping();

  return { client, connect, ping };
}
