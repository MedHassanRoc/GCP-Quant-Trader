{{ config(materialized='table', partition_by={'field':'dt','data_type':'date'}, cluster_by=['symbol','interval_']) }}

with b as (
  select * from {{ ref('int__ohlcv_clean') }}
  where symbol='BTCUSDT' and interval_='1h'
),
g as (
  select
    timestamp_trunc(timestamp, hour) as ts_hour,
    timestamp_trunc(timestamp, hour*4) as ts_4h,
    *
  from b
)
select
  ts_4h as timestamp,
  date(ts_4h) as dt,
  symbol, '4h' as interval_,
  any_value(open ignore nulls)  over w as open,
  max(high)  over w as high,
  min(low)   over w as low,
  any_value(close) over w_last as close,
  sum(volume) over w as volume,
  'binance' as source
from g
window w as (partition by ts_4h order by ts_hour rows between current row and 3 following),
       w_last as (partition by ts_4h order by ts_hour rows between 3 following and 3 following)
qualify row_number() over (partition by ts_4h order by ts_hour) = 1
order by timestamp
