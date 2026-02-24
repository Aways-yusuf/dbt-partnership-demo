-- Intermediate: Payment Method (pass-through from staging).
{{ config(materialized='view') }}
select * from {{ ref('stg_payment_method') }}
