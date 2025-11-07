{{ config(materialized='table') }}

with s as (
  select *
  from {{ ref('int__ohlcv_clean') }}
  where symbol='BTCUSDT' and interval_='1h'
),

b as (
  select
    timestamp, date(timestamp) as dt, symbol, interval_, open, high, low, close, volume,
    avg(close) over (order by timestamp rows between 19 preceding and current row) as bb_mid_20,
    stddev_samp(close) over (order by timestamp rows between 19 preceding and current row) as bb_std_20,
    greatest(high - low,
             abs(high - lag(close) over (order by timestamp)),
             abs(low  - lag(close) over (order by timestamp))) as tr
  from s
),

b2 as (
  select
    *,
    2*bb_std_20 as bb_dev_20,
    avg(tr) over (order by timestamp rows between 13 preceding and current row) as atr_14
  from b
),

-- precompute previous close (no nested analytics in next step)
lagged as (
  select
    *,
    lag(close) over (order by timestamp) as prev_close
  from b2
),

vwap_obv as (
  select
    *,
    -- VWAP(30): ratio of two window sums (allowed)
    sum(close*volume) over (order by timestamp rows between 29 preceding and current row)
      / nullif(sum(volume) over (order by timestamp rows between 29 preceding and current row), 0) as vwap_30,
    -- OBV: running sum over a non-analytic expression that uses prev_close (already computed)
    sum(
      case
        when prev_close is null then 0
        when close > prev_close then volume
        when close < prev_close then -volume
        else 0
      end
    ) over (order by timestamp) as obv
  from lagged
)

select
  timestamp,
  dt,
  symbol,
  interval_,
  open, high, low, close, volume,
  bb_mid_20, bb_std_20, bb_dev_20,
  atr_14, vwap_30, obv,
  (bb_mid_20 - bb_dev_20) as bb_low_20,
  (bb_mid_20 + bb_dev_20) as bb_high_20
from vwap_obv
order by timestamp
