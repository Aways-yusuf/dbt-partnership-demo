-- Staging: Purchasing.Suppliers (source for Dimension.Supplier). Replaces GetSupplierUpdates → Supplier_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'suppliers') }}
),
renamed as (
    select
        supplier_id as wwi_supplier_id,
        supplier_name as supplier,
        supplier_category_id,
        primary_contact_person_id,
        supplier_reference,
        payment_days,
        delivery_postal_code as postal_code,
        valid_from,
        valid_to
    from source
)
select * from renamed