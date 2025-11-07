select *
from {{ ref('int__ohlcv_clean') }}
where high < low
