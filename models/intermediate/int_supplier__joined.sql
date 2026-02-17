-- Intermediate: Supplier + Category + Primary Contact (point-in-time).
-- Replaces GetSupplierUpdates logic: SCD Type 2 with Category and Primary Contact from lookups.
-- Uses actual source column names: PascalCase (SupplierCategoryID, SupplierCategoryName, FullName, etc.).
{{ config(materialized='view') }}
with suppliers as (select * from {{ ref('stg_purchasing__suppliers') }}),
     categories as (
         select
             safe_cast(SupplierCategoryID as int64) as SupplierCategoryID,
             SupplierCategoryName,
             safe_cast(ValidFrom as timestamp) as ValidFrom,
             safe_cast(ValidTo as timestamp) as ValidTo
         from {{ source('wwi_oltp', 'SupplierCategories') }}
     ),
     people as (
         select
             safe_cast(PersonID as int64) as PersonID,
             FullName,
             ValidFrom,
             ValidTo
         from {{ source('wwi_oltp', 'People') }}
     ),
supplier_enriched as (
    select
        s.wwi_supplier_id,
        s.supplier,
        s.postal_code,
        s.supplier_reference,
        s.payment_days,
        s.ValidFrom as valid_from,
        s.ValidTo as valid_to,
        coalesce(sc.SupplierCategoryName, 'Unknown') as category,
        coalesce(p.FullName, '') as primary_contact
    from suppliers s
    left join categories sc
        on safe_cast(s.SupplierCategoryID as int64) = sc.SupplierCategoryID
        and sc.ValidFrom <= s.ValidFrom
        and (sc.ValidTo is null or sc.ValidTo > s.ValidFrom)
    left join people p
        on safe_cast(s.PrimaryContactPersonID as int64) = p.PersonID
        and p.ValidFrom <= s.ValidFrom
        and (p.ValidTo is null or p.ValidTo > s.ValidFrom)
),
with_valid_to as (
    select
        wwi_supplier_id,
        supplier,
        category,
        primary_contact,
        supplier_reference,
        payment_days,
        postal_code,
        valid_from,
        coalesce(lead(valid_from) over (partition by wwi_supplier_id order by valid_from), timestamp('9999-12-31 23:59:59.999999')) as valid_to
    from supplier_enriched
)
select * from with_valid_to
