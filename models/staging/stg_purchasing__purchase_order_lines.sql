-- Staging: Purchasing.PurchaseOrders + PurchaseOrderLines (source for Fact.Purchase). Replaces GetPurchaseUpdates → Purchase_Staging.
{{ config(materialized='view') }}
with po as (select * from {{ source('wwi_oltp', 'PurchaseOrders') }}),
     pol as (select * from {{ source('wwi_oltp', 'PurchaseOrderLines') }}),
     si as (select stock_item_id, quantity_per_outer from {{ source('wwi_oltp', 'StockItems') }}),
     pt as (select package_type_id, package_type_name from {{ source('wwi_oltp', 'PackageTypes') }})
select
    cast(po.order_date as date) as date_key,
    po.supplier_id as wwi_supplier_id,
    pol.stock_item_id as wwi_stock_item_id,
    po.purchase_order_id as wwi_purchase_order_id,
    pol.ordered_outers,
    pol.ordered_outers * coalesce(si.quantity_per_outer, 1) as ordered_quantity,
    pol.received_outers,
    coalesce(pt.package_type_name, 'Unknown') as package,
    pol.is_order_line_finalized as is_order_finalized,
    greatest(coalesce(pol.last_edited_when, po.last_edited_when), coalesce(po.last_edited_when, pol.last_edited_when)) as last_modified_when
from po
join pol on po.purchase_order_id = pol.purchase_order_id
left join si on pol.stock_item_id = si.stock_item_id
left join pt on pol.package_type_id = pt.package_type_id