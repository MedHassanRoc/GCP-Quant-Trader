with src as (
  select
    -- Convert INT64 ns → TIMESTAMP
    TIMESTAMP_MICROS(DIV(`timestamp`, 1000)) as ts,
    cast(`open`     as float64) as open,
    cast(`high`     as float64) as high,
    cast(`low`      as float64) as low,
    cast(`close`    as float64) as close,
    cast(`volume`   as float64) as volume,
    cast(`symbol`   as string)  as symbol,
    cast(`interval` as string)  as interval_,
    cast(`source`   as string)  as source
  from `leadmyroad`.`quantedge`.`ohlcv_ext`
)
select
  ts as timestamp,
  date(ts) as dt,
  open, high, low, close, volume,
  symbol, interval_, source
from src