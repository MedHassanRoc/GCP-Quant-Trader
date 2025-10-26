{{ config(materialized='table', labels={"layer":"mart"}) }}

-- source (use int__ohlcv_1h if you built it; otherwise keep int__ohlcv_clean)
with s as (
  select *
  from {{ ref('int__ohlcv_clean') }}
  where symbol = 'BTCUSDT' and interval_ = '1h'
),

base as (
  select
    timestamp,
    date(timestamp) as dt,
    symbol, interval_,
    close,
    -- 1-step return
    safe_divide(close - lag(close) over (order by timestamp),
                lag(close)           over (order by timestamp)) as r1,
    -- SMAs for reference
    avg(close) over (order by timestamp rows between 23 preceding and current row)  as sma_24,
    avg(close) over (order by timestamp rows between 95 preceding and current row)  as sma_96,
    -- Δ for RSI
    close - lag(close) over (order by timestamp) as diff
  from s
),

rsi_prep as (
  select
    *,
    greatest(diff, 0) as gain,
    abs(least(diff, 0)) as loss
  from base
),

rsi as (
  select
    *,
    avg(gain) over (order by timestamp rows between 13 preceding and current row) as avg_gain_14,
    avg(loss) over (order by timestamp rows between 13 preceding and current row) as avg_loss_14
  from rsi_prep
),

-- First window: compute the two “EMA-ish” SMAs and MACD diff
macd1 as (
  select
    *,
    avg(close) over (order by timestamp rows between 11 preceding and current row) as sma12,
    avg(close) over (order by timestamp rows between 25 preceding and current row) as sma26
  from rsi
),

-- Second window: moving average over the MACD diff (no nested analytic)
macd2 as (
  select
    *,
    (sma12 - sma26) as macd_sma
  from macd1
),

macd3 as (
  select
    *,
    avg(macd_sma) over (order by timestamp rows between 8 preceding and current row) as macd_signal_sma
  from macd2
)

select
  timestamp,
  dt,
  symbol,
  interval_,
  close,
  r1,
  sma_24,
  sma_96,
  case
    when avg_loss_14 is null or avg_loss_14 = 0 then null
    else 100.0 - 100.0 / (1.0 + (avg_gain_14 / nullif(avg_loss_14,0)))
  end as rsi_14,
  stddev_samp(r1) over (order by timestamp rows between 29 preceding and current row) as vol_30,
  macd_sma,
  macd_signal_sma
from macd3
order by timestamp
