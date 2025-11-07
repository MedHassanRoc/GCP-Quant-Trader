{{ config(materialized='table', schema='dq') }}

{{ dq_freshness_history_sql() }}
