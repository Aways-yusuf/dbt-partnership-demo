-- Staging: Purchasing.Suppliers (source for Dimension.Supplier). Replaces GetSupplierUpdates → Supplier_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'Suppliers') }}
),
renamed as (
    select
        supplierid as wwi_supplier_id,
        suppliername as supplier,
        suppliercategoryid as supplier_category_id,
        primarycontactpersonid as primary_contact_person_id,
        supplierreference as supplier_reference,
        paymentdays as payment_days,
        deliverypostalcode as postal_code,
        validfrom,
        validto
    from source
)
select * from renamed