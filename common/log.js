// common/log.js
import process from 'node:process';

const VERBOSE = (process.env.VERBOSE || '1') !== '0';
const NAME = process.env.SVC_NAME ? `[${process.env.SVC_NAME}]` : '';

const t = () => new Date().toISOString();

export function debug(...a) { if (VERBOSE) console.log('[debug]', t(), NAME, ...a); }
export function info (...a) { console.log('[info ]', t(), NAME, ...a); }
export function warn (...a) { console.warn('[warn ]', t(), NAME, ...a); }
export function err  (...a) { console.error('[error]', t(), NAME, ...a); }

process.on('unhandledRejection', (e) => err('unhandledRejection', e?.stack || e));
process.on('uncaughtException',  (e) => err('uncaughtException',  e?.stack || e));

export default { debug, info, warn, err };
