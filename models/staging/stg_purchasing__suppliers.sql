-- Staging: Purchasing.Suppliers (source for Dimension.Supplier). Replaces GetSupplierUpdates → Supplier_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'Suppliers') }}
),
renamed as (
    select
        safe_cast(supplierid as int64) as wwi_supplier_id,
        suppliername as supplier,
        safe_cast(suppliercategoryid as int64) as suppliercategoryid,
        safe_cast(primarycontactpersonid as int64) as primarycontactpersonid,
        supplierreference as supplier_reference,
        paymentdays as payment_days,
        deliverypostalcode as postal_code,
        safe_cast(substr(cast(validfrom as string), 1, 26) as timestamp) as valid_from,
        safe_cast(substr(cast(validto as string), 1, 26) as timestamp) as valid_to
    from source
)
select * from renamed