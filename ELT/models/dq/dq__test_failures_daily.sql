{{ config(materialized='table', schema='dq') }}

with unioned as (
  {{ dq_union_failure_tables() }}
)
select * from unioned
