// services/worker-metadata/index.js
import 'dotenv/config';
import { info } from '../../common/log.js';
import { tsInit } from '../../common/db-timescale.js';

// tasks
import { startMetaRefresher, startIbcMetaRefresher } from './tasks/meta-refresher.js';
import { startHoldersRefresher } from './tasks/holders-refresher.js';
import { startPriceFromReserves } from './tasks/price-from-reserves.js';
import matrix from './tasks/matrix-rollups.js';
import { startTokenSecurityScanner } from './tasks/token-security.js';
import { startFx } from './tasks/fx-zig.js';
import { startAlertsEngine } from './tasks/alerts.js';

// fast-track consumer (Redis stream: events:new_pool)
import { startFasttrackConsumer } from './tasks/fasttrack-consumer.js';

(async function main () {
  await tsInit();
  info('[worker-metadata] starting…');

  // long-running background jobs
  startMetaRefresher();
  startIbcMetaRefresher();
  startHoldersRefresher();
  startPriceFromReserves();
  matrix.start();
  startTokenSecurityScanner();
  startFx();
  startAlertsEngine();

  // react instantly when a new pool is created
  startFasttrackConsumer(); // ← replaces old PG NOTIFY listener
})();
