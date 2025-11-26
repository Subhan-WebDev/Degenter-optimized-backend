-- Use UTC everywhere
SET timezone = 'UTC';

-- ==================== CORE ENTITIES ====================

CREATE TABLE IF NOT EXISTS degenter.tokens
(
  token_id          UInt64,
  denom             String,
  type              LowCardinality(String),
  name              String,
  symbol            String,
  display           String,
  exponent          Int16,
  image_uri         String,
  website           String,
  twitter           String,
  telegram          String,
  max_supply_base   Decimal(76,0),
  total_supply_base Decimal(76,0),
  description       String,
  created_at        DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY token_id;

CREATE TABLE IF NOT EXISTS degenter.pools
(
  pool_id          UInt64,
  pair_contract    String,
  base_token_id    UInt64,
  quote_token_id   UInt64,
  lp_token_denom   String,
  pair_type        LowCardinality(String),
  is_uzig_quote    UInt8,
  factory_contract String,
  router_contract  String,
  created_at       DateTime64(3, 'UTC'),
  created_height   UInt64,
  created_tx_hash  String,
  signer           String
)
ENGINE = MergeTree
ORDER BY pool_id;

-- ==================== TIME-SERIES ====================

CREATE TABLE IF NOT EXISTS degenter.trades
(
  trade_id                    UInt64,
  pool_id                     UInt64,
  pair_contract               String,
  action                      LowCardinality(String),
  direction                   LowCardinality(String),
  offer_asset_denom           String,
  offer_amount_base           Decimal(76,0),
  ask_asset_denom             String,
  ask_amount_base             Decimal(76,0),
  return_amount_base          Decimal(76,0),
  is_router                   UInt8,
  reserve_asset1_denom        String,
  reserve_asset1_amount_base  Decimal(76,0),
  reserve_asset2_denom        String,
  reserve_asset2_amount_base  Decimal(76,0),
  height                      UInt64,
  tx_hash                     String,
  signer                      String,
  msg_index                   Int32,
  created_at                  DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (pool_id, created_at, tx_hash, msg_index);

CREATE TABLE IF NOT EXISTS degenter.price_ticks
(
  pool_id       UInt64,
  token_id      UInt64,
  price_in_zig  Decimal(36,18),
  ts            DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (pool_id, ts);

CREATE TABLE IF NOT EXISTS degenter.ohlcv_1m
(
  pool_id        UInt64,
  bucket_start   DateTime64(3, 'UTC'),
  open           Decimal(36,18),
  high           Decimal(36,18),
  low            Decimal(36,18),
  close          Decimal(36,18),
  volume_zig     Decimal(36,8),
  trade_count    UInt32,
  liquidity_zig  Decimal(36,8)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(bucket_start)
ORDER BY (pool_id, bucket_start);

-- ==================== STATE / ROLLUPS ====================

CREATE TABLE IF NOT EXISTS degenter.prices
(
  price_id       UInt64,
  token_id       UInt64,
  pool_id        UInt64,
  price_in_zig   Decimal(36,18),
  is_pair_native UInt8,
  updated_at     DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (token_id, pool_id);

CREATE TABLE IF NOT EXISTS degenter.pool_state
(
  pool_id            UInt64,
  reserve_base_base  Decimal(76,0),
  reserve_quote_base Decimal(76,0),
  updated_at         DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY pool_id;

CREATE TABLE IF NOT EXISTS degenter.pool_matrix
(
  pool_id            UInt64,
  bucket             LowCardinality(String), -- '30m','1h','4h','24h'
  vol_buy_quote      Decimal(36,8),
  vol_sell_quote     Decimal(36,8),
  vol_buy_zig        Decimal(36,8),
  vol_sell_zig       Decimal(36,8),
  tx_buy             UInt32,
  tx_sell            UInt32,
  unique_traders     UInt32,
  tvl_zig            Decimal(36,8),
  reserve_base_disp  Decimal(36,18),
  reserve_quote_disp Decimal(36,18),
  updated_at         DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (pool_id, bucket);

CREATE TABLE IF NOT EXISTS degenter.token_matrix
(
  token_id     UInt64,
  bucket       LowCardinality(String), -- '30m','1h','4h','24h'
  price_in_zig Decimal(36,18),
  mcap_zig     Decimal(36,8),
  fdv_zig      Decimal(36,8),
  holders      UInt64,
  updated_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (token_id, bucket);

-- ==================== HOLDERS ====================

CREATE TABLE IF NOT EXISTS degenter.holders
(
  token_id      UInt64,
  address       String,
  balance_base  Decimal(76,0),
  updated_at    DateTime64(3, 'UTC'),
  last_seen_height UInt64
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (token_id, address);

CREATE TABLE IF NOT EXISTS degenter.token_holders_stats
(
  token_id     UInt64,
  holders_count UInt64,
  updated_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY token_id;

-- ==================== LEADERBOARDS / OUTLIERS ====================

CREATE TABLE IF NOT EXISTS degenter.leaderboard_traders
(
  bucket        LowCardinality(String),
  address       String,
  trades_count  UInt32,
  volume_zig    Decimal(36,8),
  gross_pnl_zig Decimal(36,8),
  updated_at    DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (bucket, address);

CREATE TABLE IF NOT EXISTS degenter.large_trades
(
  id           UInt64,
  bucket       LowCardinality(String),
  pool_id      UInt64,
  tx_hash      String,
  signer       String,
  value_zig    Decimal(36,8),
  direction    LowCardinality(String),
  created_at   DateTime64(3, 'UTC'),
  inserted_at  DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (bucket, created_at, pool_id, tx_hash);

-- ==================== FX / STATE ====================

CREATE TABLE IF NOT EXISTS degenter.exchange_rates
(
  ts      DateTime64(3, 'UTC'),
  zig_usd Decimal(36,8)
)
ENGINE = ReplacingMergeTree(ts)
ORDER BY ts;

CREATE TABLE IF NOT EXISTS degenter.index_state
(
  id          String,
  last_height UInt64,
  updated_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;

-- ==================== WALLETS / ALERTS ====================

CREATE TABLE IF NOT EXISTS degenter.wallets
(
  wallet_id    UInt64,
  address      String,
  display_name String,
  created_at   DateTime64(3, 'UTC'),
  last_seen    DateTime64(3, 'UTC'),
  last_seen_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY wallet_id;

CREATE TABLE IF NOT EXISTS degenter.watchlist
(
  id         UInt64,
  wallet_id  UInt64,
  token_id   UInt64,
  pool_id    UInt64,
  note       String,
  created_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (wallet_id, token_id, pool_id, id);

CREATE TABLE IF NOT EXISTS degenter.alerts
(
  alert_id       UInt64,
  wallet_id      UInt64,
  alert_type     LowCardinality(String),
  params         String,                 -- JSON text
  is_active      UInt8,
  throttle_sec   UInt32,
  last_triggered DateTime64(3, 'UTC'),
  created_at     DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (wallet_id, alert_id);

CREATE TABLE IF NOT EXISTS degenter.alert_events
(
  id           UInt64,
  alert_id     UInt64,
  wallet_id    UInt64,
  kind         LowCardinality(String),
  payload      String,                   -- JSON text
  triggered_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(triggered_at)
ORDER BY (alert_id, triggered_at, id);

-- ==================== TOKEN TWITTER ====================

CREATE TABLE IF NOT EXISTS degenter.token_twitter
(
  token_id           UInt64,
  handle             String,
  user_id            String,
  profile_url        String,
  name               String,
  is_blue_verified   UInt8,
  verified_type      String,
  profile_picture    String,
  cover_picture      String,
  description        String,
  location           String,
  followers          UInt64,
  following          UInt64,
  favourites_count   UInt64,
  statuses_count     UInt64,
  media_count        UInt64,
  can_dm             UInt8,
  created_at_twitter DateTime64(3, 'UTC'),
  possibly_sensitive UInt8,
  is_automated       UInt8,
  automated_by       String,
  pinned_tweet_ids   Array(String),
  unavailable        UInt8,
  unavailable_message String,
  unavailable_reason  String,
  raw                String,                    -- JSON text
  last_refreshed     DateTime64(3, 'UTC'),
  last_error         String,
  last_error_at      DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(last_refreshed)
ORDER BY token_id;
