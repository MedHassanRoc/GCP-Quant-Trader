select *
from {{ ref('int__ohlcv_clean') }}
where open < 0 or high < 0 or low < 0 or close < 0 or volume < 0
