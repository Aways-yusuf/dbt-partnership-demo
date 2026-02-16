-- Staging: Purchasing.PurchaseOrders + PurchaseOrderLines (source for Fact.Purchase). Replaces GetPurchaseUpdates → Purchase_Staging.
{{ config(materialized='view') }}
with po as (select * from {{ source('wwi_oltp', 'PurchaseOrders') }}),
     pol as (select * from {{ source('wwi_oltp', 'PurchaseOrderLines') }}),
     si as (select stockitemid, quantityperouter from {{ source('wwi_oltp', 'StockItems') }}),
     pt as (select packagetypeid, packagetypename from {{ source('wwi_oltp', 'PackageTypes') }})
select
    cast(po.orderdate as date) as date_key,
    po.supplierid as wwi_supplier_id,
    pol.stockitemid as wwi_stock_item_id,
    po.purchaseorderid as wwi_purchase_order_id,
    pol.orderedouters,
    pol.orderedouters * coalesce(si.quantityperouter, 1) as ordered_quantity,
    pol.receivedouters,
    coalesce(pt.packagetypename, 'Unknown') as package,
    pol.isorderlinefinalized as is_order_finalized,
    greatest(coalesce(pol.lasteditedwhen, po.lasteditedwhen), coalesce(po.lasteditedwhen, pol.lasteditedwhen)) as last_modified_when
from po
join pol on po.purchaseorderid = pol.purchaseorderid
left join si on pol.stockitemid = si.stockitemid
left join pt on pol.packagetypeid = pt.packagetypeid