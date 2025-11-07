{% macro dq_freshness_history_sql() %}
  {% set dq_schema = target.schema ~ '_dq' %}

  {% if execute %}
    {% set check_sql -%}
      select 1
      from `{{ target.project }}.{{ dq_schema }}`.INFORMATION_SCHEMA.TABLES
      where table_name = 'freshness_log'
      limit 1
    {%- endset %}
    {% set res = run_query(check_sql) %}

    {% if res is not none and res.rows|length > 0 %}
      select
        datetime(generated_at) as generated_at,
        source_name,
        status,
        max_loaded_at,
        max_loaded_at_time_ago_in_s/3600.0 as hours_since_last_load
      from `{{ target.project }}.{{ dq_schema }}.freshness_log`
    {% else %}
      select
        cast(null as datetime) as generated_at,
        cast(null as string)   as source_name,
        cast(null as string)   as status,
        cast(null as timestamp)as max_loaded_at,
        cast(null as float64)  as hours_since_last_load
      limit 0
    {% endif %}
  {% else %}
    select
      cast(null as datetime) as generated_at,
      cast(null as string)   as source_name,
      cast(null as string)   as status,
      cast(null as timestamp)as max_loaded_at,
      cast(null as float64)  as hours_since_last_load
    limit 0
  {% endif %}
{% endmacro %}
