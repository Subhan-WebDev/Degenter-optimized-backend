// services/processor-core/parser/parse.js
import {
  digitsOrNull, wasmByAction, byType, buildMsgSenderMap, normalizePair,
  classifyDirection, parseReservesKV, parseAssetsList, sha256hex
} from '../../../common/core/parse.js';

const FACTORY_ADDR = process.env.FACTORY_ADDR || '';   // set in .env when available
const ROUTER_ADDR  = process.env.ROUTER_ADDR  || '';   // optional

export async function parseBlock(raw) {
  // raw = { height, time, header, txs, results }
  const height    = Number(raw?.height || raw?.header?.height || 0);
  const timestamp = raw?.time || raw?.header?.time || null;
  const txs       = Array.isArray(raw?.txs) ? raw.txs : [];
  const hashes    = txs.map(sha256hex);
  const txResults = Array.isArray(raw?.results) ? raw.results : [];

  const pools   = [];
  const swaps   = [];
  const liqs    = [];
  const prices  = []; // reserve snapshots (writers will compute price / ohlcv)

  const N = Math.max(txResults.length, hashes.length);

  for (let i = 0; i < N; i++) {
    const txr     = txResults[i] || { events: [] };
    const tx_hash = hashes[i] || null;

    const wasms    = byType(txr.events, 'wasm');
    const insts    = byType(txr.events, 'instantiate');
    const executes = byType(txr.events, 'execute');
    const msgs     = byType(txr.events, 'message');
    const msgSenderByIndex = buildMsgSenderMap(msgs);

    // ───────────────────────────────────────────────────────────────
    // create_pair → pools
    // ───────────────────────────────────────────────────────────────
    const cps = wasmByAction(wasms, 'create_pair');
    for (const cp of cps) {
      // If FACTORY_ADDR is known, enforce it; otherwise accept (dev mode)
      const addrOk = FACTORY_ADDR
        ? (String(cp.m.get('_contract_address') || '').trim() === FACTORY_ADDR)
        : true;
      if (!addrOk) continue;

      const pairType = String(cp.m.get('pair_type') || 'xyk');
      const { base, quote } = normalizePair(cp.m.get('pair'));

      // pair contract address can be in a preceding 'register' wasm or final instantiate
      const reg = wasms.find(w =>
        w.m.get('action') === 'register' &&
        (!FACTORY_ADDR || w.m.get('_contract_address') === FACTORY_ADDR)
      );
      const pairContract = reg?.m.get('pair_contract_addr') || insts.at(-1)?.m.get('_contract_address');
      if (!pairContract) continue;

      const signer = msgSenderByIndex.get(Number(cp.m.get('msg_index'))) || null;

      pools.push({
        pair_contract: pairContract,
        base_denom: base, quote_denom: quote,
        pair_type: pairType,
        created_at: timestamp,
        height, tx_hash, signer
      });
    }

    // ───────────────────────────────────────────────────────────────
    // swaps
    // ───────────────────────────────────────────────────────────────
    const sEvs = wasmByAction(wasms, 'swap');
    for (let idx = 0; idx < sEvs.length; idx++) {
      const s = sEvs[idx];
      const pair_contract = s.m.get('_contract_address');
      if (!pair_contract) continue;

      const offer_asset_denom = s.m.get('offer_asset') || s.m.get('offer_asset_denom') || null;
      const ask_asset_denom   = s.m.get('ask_asset')   || s.m.get('ask_asset_denom')   || null;
      const offer_amount_base = digitsOrNull(s.m.get('offer_amount'));
      const ask_amount_base   = digitsOrNull(s.m.get('ask_amount'));
      const return_amount_base= digitsOrNull(s.m.get('return_amount'));

      // reserves (various shapes)
      let res1d = s.m.get('reserve_asset1_denom') || s.m.get('asset1_denom') || null;
      let res1a = digitsOrNull(s.m.get('reserve_asset1_amount') || s.m.get('asset1_amount'));
      let res2d = s.m.get('reserve_asset2_denom') || s.m.get('asset2_denom') || null;
      let res2a = digitsOrNull(s.m.get('reserve_asset2_amount') || s.m.get('asset2_amount'));
      const reservesStr = s.m.get('reserves');
      if ((!res1d || !res1a || !res2d || !res2a) && reservesStr) {
        const kv = parseReservesKV(reservesStr);
        if (kv?.[0]) { res1d = res1d ?? kv[0].denom; res1a = res1a ?? digitsOrNull(kv[0].amount_base); }
        if (kv?.[1]) { res2d = res2d ?? kv[1].denom; res2a = res2a ?? digitsOrNull(kv[1].amount_base); }
      }

      const msg_index   = Number(s.m.get('msg_index') ?? idx);
      const event_index = idx; // disambiguate multiple swap events within same msg
      const signer      = msgSenderByIndex.get(msg_index) || null;

      const poolSwapSender = s.m.get('sender') || null;
      const routerExec = !!ROUTER_ADDR && executes.some(e =>
        e.m.get('_contract_address') === ROUTER_ADDR &&
        Number(e.m.get('msg_index') || -1) === msg_index
      );
      const is_router = !!(ROUTER_ADDR && (poolSwapSender === ROUTER_ADDR || routerExec));

      swaps.push({
        pair_contract,
        action: 'swap',
        offer_asset_denom, offer_amount_base,
        ask_asset_denom,   ask_amount_base,
        return_amount_base,
        is_router,
        reserve_asset1_denom: res1d, reserve_asset1_amount_base: res1a,
        reserve_asset2_denom: res2d, reserve_asset2_amount_base: res2a,
        height, tx_hash, signer, msg_index, event_index, created_at: timestamp
      });

      // emit a price “input” event when we have both reserves
      if (res1d && res1a && res2d && res2a) {
        prices.push({
          kind: 'reserves_snapshot',
          pair_contract,
          reserves: [
            { denom: res1d, amount_base: res1a },
            { denom: res2d, amount_base: res2a }
          ],
          at: timestamp,
          height,
          tx_hash,
          source: 'swap'
        });
      }
    }

    // ───────────────────────────────────────────────────────────────
    // liquidity (provide/withdraw)
    // ───────────────────────────────────────────────────────────────
    const provides  = wasmByAction(wasms, 'provide_liquidity');
    const withdraws = wasmByAction(wasms, 'withdraw_liquidity');
    const liqEvs    = [...provides, ...withdraws];

    for (let li = 0; li < liqEvs.length; li++) {
      const le = liqEvs[li];
      const pair_contract = le.m.get('_contract_address');
      if (!pair_contract) continue;

      const isProvide = (le.m.get('action') === 'provide_liquidity');
      const action    = isProvide ? 'provide' : 'withdraw';

      let res1d = le.m.get('reserve_asset1_denom') || null;
      let res1a = digitsOrNull(le.m.get('reserve_asset1_amount'));
      let res2d = le.m.get('reserve_asset2_denom') || null;
      let res2a = digitsOrNull(le.m.get('reserve_asset2_amount'));

      const assetsStr = isProvide ? le.m.get('assets') : le.m.get('refund_assets');
      if ((!res1d || !res1a || !res2d || !res2a) && assetsStr) {
        const parsed = parseAssetsList(assetsStr);
        if (parsed?.a1) { res1d = res1d ?? parsed.a1.denom; res1a = res1a ?? digitsOrNull(parsed.a1.amount_base); }
        if (parsed?.a2) { res2d = res2d ?? parsed.a2.denom; res2a = res2a ?? digitsOrNull(parsed.a2.amount_base); }
      }

      const reservesStr = le.m.get('reserves');
      if ((!res1d || !res1a || !res2d || !res2a) && reservesStr) {
        const kv = parseReservesKV(reservesStr);
        if (kv?.[0]) { res1d = res1d ?? kv[0].denom; res1a = res1a ?? digitsOrNull(kv[0].amount_base); }
        if (kv?.[1]) { res2d = res2d ?? kv[1].denom; res2a = res2a ?? digitsOrNull(kv[1].amount_base); }
      }

      const share_base = digitsOrNull(
        isProvide
          ? (le.m.get('share'))
          : (le.m.get('withdrawn_share') || le.m.get('withdraw_share') ||
             le.m.get('liquidity')      || le.m.get('burn_share')     ||
             le.m.get('burnt_share')    || le.m.get('share'))
      );

      const msg_index   = Number(le.m.get('msg_index') ?? li);
      const event_index = li; // disambiguate multiple liquidity events within same msg
      const signer      = msgSenderByIndex.get(msg_index) || null;

      liqs.push({
        pair_contract,
        action,
        share_base,
        reserve_asset1_denom: res1d, reserve_asset1_amount_base: res1a,
        reserve_asset2_denom: res2d, reserve_asset2_amount_base: res2a,
        height, tx_hash, signer, msg_index, event_index, created_at: timestamp
      });

      if (res1d && res1a && res2d && res2a) {
        prices.push({
          kind: 'reserves_snapshot',
          pair_contract,
          reserves: [
            { denom: res1d, amount_base: res1a },
            { denom: res2d, amount_base: res2a }
          ],
          at: timestamp,
          height,
          tx_hash,
          source: action
        });
      }
    }
  }

  return { pools, swaps, liqs, prices };
}

export default { parseBlock };
