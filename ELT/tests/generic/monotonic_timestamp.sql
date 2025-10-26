{% test monotonic_timestamp(model, column_name) %}
with x as (
  select {{ column_name }} as ts,
         lag({{ column_name }}) over (order by {{ column_name }}) as prev_ts
  from {{ model }}
)
select *
from x
where prev_ts is not null and ts < prev_ts
{% endtest %}
