-- Intermediate: Movement (pass-through from staging).
{{ config(materialized='view') }}
select * from {{ ref('stg_movement') }}
