-- Staging: Purchasing.Suppliers (source for Dimension.Supplier). Replaces GetSupplierUpdates → Supplier_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'Suppliers') }}
),
renamed as (
    select
        {{ cross_db_safe_cast_int('supplierid') }} as wwi_supplier_id,
        suppliername as supplier,
        {{ cross_db_safe_cast_int('suppliercategoryid') }} as suppliercategoryid,
        {{ cross_db_safe_cast_int('primarycontactpersonid') }} as primarycontactpersonid,
        supplierreference as supplier_reference,
        paymentdays as payment_days,
        deliverypostalcode as postal_code,
        {{ cross_db_cast_timestamp('validfrom') }} as valid_from,
        {{ cross_db_cast_timestamp('validto') }} as valid_to
    from source
)
select * from renamed
