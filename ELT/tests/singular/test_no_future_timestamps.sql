select *
from {{ ref('int__ohlcv_clean') }}
where timestamp > current_timestamp()
