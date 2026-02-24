-- Intermediate: Stock Holding (pass-through from staging).
{{ config(materialized='view') }}
select * from {{ ref('stg_stock_holding') }}
