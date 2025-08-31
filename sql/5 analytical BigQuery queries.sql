-- Top coins by 24h volume at the most recent snapshot
WITH latest AS (
  SELECT MAX(last_updated) AS max_ts
  FROM `learn-arflow.crypto_db.tbl_crypto`
)
SELECT
  symbol,
  name,
  total_volume,
  market_cap,
  current_price
FROM `learn-arflow.crypto_db.tbl_crypto` t
CROSS JOIN latest
WHERE t.last_updated = latest.max_ts
QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY last_updated DESC) = 1
ORDER BY total_volume DESC
LIMIT 20;

-- Change 'btc' to any symbol you want
DECLARE p_symbol STRING DEFAULT 'btc';

WITH daily AS (
  SELECT
    DATE(last_updated) AS d,
    ANY_VALUE(current_price) AS price
  FROM `learn-arflow.crypto_db.tbl_crypto`
  WHERE LOWER(symbol) = LOWER(p_symbol)
  GROUP BY d
)
SELECT
  d AS date,
  price AS close_price,
  AVG(price) OVER (ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma_7d,
  100 * (price / LAG(price) OVER (ORDER BY d) - 1) AS daily_return_pct
FROM daily
ORDER BY date;

--30-day annualized volatility (log-returns) per coin
WITH daily AS (
  SELECT
    symbol,
    DATE(last_updated) AS d,
    ANY_VALUE(current_price) AS price
  FROM `learn-arflow.crypto_db.tbl_crypto`
  GROUP BY symbol, d
),
rets AS (
  SELECT
    symbol,
    d,
    SAFE.LOG(price / LAG(price) OVER (PARTITION BY symbol ORDER BY d)) AS log_ret
  FROM daily
)
SELECT
  symbol,
  STDDEV_SAMP(log_ret) * SQRT(365) AS ann_vol_30d   -- annualized
FROM rets
WHERE d >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY symbol
ORDER BY ann_vol_30d DESC
LIMIT 20;

--Market-cap dominance (share of total mcap) for latest day
WITH latest_day AS (
  SELECT DATE(MAX(last_updated)) AS d
  FROM `learn-arflow.crypto_db.tbl_crypto`
),
snap AS (
  SELECT
    symbol,
    name,
    ANY_VALUE(market_cap) AS market_cap
  FROM `learn-arflow.crypto_db.tbl_crypto`, latest_day
  WHERE DATE(last_updated) = latest_day.d
  GROUP BY symbol, name
),
tot AS (
  SELECT SUM(market_cap) AS total_mcap FROM snap
)
SELECT
  symbol,
  name,
  market_cap,
  market_cap / total_mcap AS dominance_share
FROM snap, tot
ORDER BY dominance_share DESC
LIMIT 10;

--Correlation of daily returns (BTC vs. ETH) over the last 90 days
WITH daily AS (
  SELECT
    LOWER(symbol) AS symbol,
    DATE(last_updated) AS d,
    ANY_VALUE(current_price) AS price
  FROM `learn-arflow.crypto_db.tbl_crypto`
  WHERE LOWER(symbol) IN ('btc','eth')
  GROUP BY symbol, d
),
rets AS (
  SELECT
    symbol, d,
    100 * (price / LAG(price) OVER (PARTITION BY symbol ORDER BY d) - 1) AS r
  FROM daily
)
SELECT
  CORR(b.r, e.r) AS corr_btc_eth_90d
FROM
  (SELECT d, r FROM rets WHERE symbol='btc') b
JOIN
  (SELECT d, r FROM rets WHERE symbol='eth') e
USING (d)
WHERE d >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);

