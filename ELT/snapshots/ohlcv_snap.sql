{% snapshot ohlcv_snap %}
  {{
    config(
      target_database=target.project,
      target_schema=target.schema ~ '_snap',
      unique_key='concat(cast(timestamp as string), "-", symbol, "-", interval_)',
      strategy='check',
      check_cols=['open','high','low','close','volume']
    )
  }}

  -- Source rows to track (1h BTCUSDT subset; expand if needed)
  select
    timestamp,
    dt,
    symbol,
    interval_,
    open, high, low, close, volume
  from {{ ref('raw__ohlcv') }}
  where symbol='BTCUSDT' and interval_='1h'
{% endsnapshot %}
