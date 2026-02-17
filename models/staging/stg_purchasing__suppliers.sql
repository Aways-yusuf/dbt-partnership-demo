-- Staging: Purchasing.Suppliers (source for Dimension.Supplier). Replaces GetSupplierUpdates → Supplier_Staging.
-- Columns aligned to actual BigQuery schema: SupplierID, SupplierName, SupplierCategoryID,
-- PrimaryContactPersonID, SupplierReference, PaymentDays, DeliveryPostalCode (INTEGER), ValidFrom/ValidTo (TIMESTAMP).
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'Suppliers') }}
),
renamed as (
    select
        SupplierID as wwi_supplier_id,
        SupplierName as supplier,
        SupplierCategoryID,
        PrimaryContactPersonID,
        SupplierReference as supplier_reference,
        PaymentDays as payment_days,
        DeliveryPostalCode as postal_code,
        ValidFrom,
        ValidTo
    from source
)
select * from renamed
