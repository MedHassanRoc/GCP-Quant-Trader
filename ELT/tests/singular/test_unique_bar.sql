select timestamp, symbol, interval_, count(*) as cnt
from {{ ref('int__ohlcv_clean') }}
group by 1,2,3
having count(*) > 1
