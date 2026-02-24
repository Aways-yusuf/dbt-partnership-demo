-- Intermediate: Stock Item (pass-through from staging).
{{ config(materialized='view') }}
select * from {{ ref('stg_stock_item') }}
