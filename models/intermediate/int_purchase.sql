-- Intermediate: Purchase (pass-through from staging).
{{ config(materialized='view') }}
select * from {{ ref('stg_purchase') }}
