{{
  config(
    materialized='table',
    labels={"layer": "int"}
  )
}}

with base as (
  select
    timestamp,
    dt,
    cast(open  as float64)  as open,
    cast(high  as float64)  as high,
    cast(low   as float64)  as low,
    cast(close as float64)  as close,
    cast(volume as float64) as volume,
    cast(symbol as string)  as symbol,
    cast(interval_ as string) as interval_
  from {{ ref('raw__ohlcv') }}
  where symbol = 'BTCUSDT' and interval_ = '1h'
),

dedup as (
  select *
  from base
  qualify row_number() over (partition by timestamp order by timestamp desc) = 1
),

valid as (
  select *
  from dedup
  where open is not null and high is not null and low is not null and close is not null
),

monotonic as (
  select *
  from valid
  qualify timestamp = max(timestamp) over (
    partition by dt, symbol, interval_
    order by timestamp
    rows between unbounded preceding and current row
  )
)

select *
from monotonic
order by timestamp
