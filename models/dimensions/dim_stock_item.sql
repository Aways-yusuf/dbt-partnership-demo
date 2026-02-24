-- Dimension Stock Item (SCD2). Replaces MigrateStagedStockItemData → Dimension.Stock Item.
{{ config(materialized='table') }}
with si as (select * from {{ ref('int_stock_item') }}),
with_valid_to as (
    select wwi_stock_item_id, stock_item, brand, size, leadtimedays as lead_time_days, quantityperouter as quantity_per_outer, 
    ischillerstock as is_chiller_stock, taxrate as tax_rate, unitprice as unit_price, typicalweightperunit as typical_weight_per_unit, 
    validfrom as valid_from, coalesce(lead(validfrom) over (partition by wwi_stock_item_id order by validfrom), timestamp('9999-12-31 23:59:59.999999')) as valid_to
    from si
)
select row_number() over (order by wwi_stock_item_id, valid_from) as stock_item_key,
       wwi_stock_item_id, stock_item, brand, size, lead_time_days, quantity_per_outer, is_chiller_stock, tax_rate, unit_price, typical_weight_per_unit, valid_from, valid_to
from with_valid_to