{% macro dq_union_failure_tables() %}
  {% set dq_schema = target.schema ~ '_dq' %}

  {% if execute %}
    {% set list_sql -%}
      select table_name
      from `{{ target.project }}.{{ dq_schema }}`.INFORMATION_SCHEMA.TABLES
      where starts_with(table_name, 'dbt_test__')
    {%- endset %}
    {% set res = run_query(list_sql) %}

    {% if res is not none and res.rows|length > 0 %}
      {% set selects = [] %}
      {% for r in res.rows %}
        {% set t = r[0] %}
        {% do selects.append("
          select
            current_date() as as_of_date,
            '" ~ t ~ "' as test_name,
            null as model_name,
            count(*) as failed_rows
          from `{{ target.project }}.{{ dq_schema }}." ~ t ~ "`
        ") %}
      {% endfor %}
      {{ selects | join(" union all ") }}
    {% else %}
      select
        cast(null as date)   as as_of_date,
        cast(null as string) as test_name,
        cast(null as string) as model_name,
        cast(null as int64)  as failed_rows
      limit 0
    {% endif %}
  {% else %}
    select
      cast(null as date)   as as_of_date,
      cast(null as string) as test_name,
      cast(null as string) as model_name,
      cast(null as int64)  as failed_rows
    limit 0
  {% endif %}
{% endmacro %}
