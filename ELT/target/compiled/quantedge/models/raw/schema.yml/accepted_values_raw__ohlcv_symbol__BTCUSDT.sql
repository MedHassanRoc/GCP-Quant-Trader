
    
    

with all_values as (

    select
        symbol as value_field,
        count(*) as n_records

    from `leadmyroad`.`quantedge`.`raw__ohlcv`
    group by symbol

)

select *
from all_values
where value_field not in (
    'BTCUSDT'
)


