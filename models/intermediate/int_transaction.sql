-- Intermediate: Transaction (pass-through from staging union).
{{ config(materialized='view') }}
select * from {{ ref('stg_transaction') }}
