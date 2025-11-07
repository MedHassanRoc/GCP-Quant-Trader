{{ config(materialized='table') }}

with f as (
  select *
  from {{ ref('mart__features_plus') }}
  where symbol = 'BTCUSDT' and interval_ = '1h'
),
y as (
  select
    f.timestamp,
    date(f.timestamp) as dt,
    f.symbol,
    f.interval_,
    f.close,
    -- pull all feature columns except the ones we already selected
    f.* except(timestamp, dt, symbol, interval_, close),
    lead(f.close) over (order by f.timestamp) as close_fwd1
  from f
)
select
  *,
  safe_divide(close_fwd1 - close, nullif(close, 0)) as target_ret_1,
  case when close_fwd1 > close then 1 else 0 end as target_up_1,
  case
    when timestamp < timestamp_sub(current_timestamp(), interval 365 day) then 'train'
    when timestamp < timestamp_sub(current_timestamp(), interval 90  day) then 'valid'
    else 'test'
  end as split
from y
