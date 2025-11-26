// common/load-env.js
import { fileURLToPath } from 'url';
import { dirname, resolve } from 'path';
import dotenv from 'dotenv';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = dirname(__filename);

// 1) Load root .env (repo/)
dotenv.config({ path: resolve(__dirname, '../.env') });

// 2) Load service-local .env to allow overrides (optional)
dotenv.config();

export function loadedEnv(keys = []) {
  return Object.fromEntries(keys.map(k => [k, process.env[k]]));
}
