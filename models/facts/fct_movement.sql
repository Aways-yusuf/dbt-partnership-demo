-- Fact Movement. Replaces MigrateStagedMovementData → Fact.Movement.
{{ config(materialized='table') }}
with stg as (select * from {{ ref('stg_warehouse__stock_item_transactions') }}),
     dsi as (select * from {{ ref('dim_stock_item') }}),
     dcu as (select * from {{ ref('dim_customer') }}),
     dsu as (select * from {{ ref('dim_supplier') }}),
     dtt as (select * from {{ ref('dim_transaction_type') }})
select
    stg.date_key as date_key,
    coalesce((select stock_item_key from dsi where dsi.wwi_stock_item_id = stg.wwi_stock_item_id and stg.last_modified_when > dsi.valid_from and stg.last_modified_when <= dsi.valid_to order by dsi.valid_from desc limit 1), 0) as stock_item_key,
    coalesce((select customer_key from dcu where dcu.wwi_customer_id = stg.wwi_customer_id and stg.last_modified_when > dcu.valid_from and stg.last_modified_when <= dcu.valid_to order by dcu.valid_from desc limit 1), 0) as customer_key,
    coalesce((select supplier_key from dsu where dsu.wwi_supplier_id = stg.wwi_supplier_id and stg.last_modified_when > dsu.valid_from and stg.last_modified_when <= dsu.valid_to order by dsu.valid_from desc limit 1), 0) as supplier_key,
    coalesce((select transaction_type_key from dtt where dtt.wwi_transaction_type_id = stg.wwi_transaction_type_id and stg.last_modified_when > dtt.valid_from and stg.last_modified_when <= dtt.valid_to order by dtt.valid_from desc limit 1), 0) as transaction_type_key,
    stg.wwi_stock_item_transaction_id,
    stg.wwi_invoice_id,
    stg.wwi_purchase_order_id,
    stg.quantity
from stg