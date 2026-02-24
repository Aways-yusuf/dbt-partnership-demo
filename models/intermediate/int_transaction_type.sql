-- Intermediate: Transaction Type (pass-through from staging).
{{ config(materialized='view') }}
select * from {{ ref('stg_transaction_type') }}
