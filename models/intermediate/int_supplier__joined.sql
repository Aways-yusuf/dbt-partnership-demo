-- Intermediate: Supplier + Category + Primary Contact (point-in-time).
-- Replaces GetSupplierUpdates logic: SCD Type 2 with Category and Primary Contact from lookups.
-- Dependency: Stock Item (load order only; Supplier runs after Stock Item in SSIS).
{{ config(materialized='view', schema='intermediate') }}
with suppliers as (select * from {{ ref('stg_purchasing__suppliers') }}),
     categories as (select * from {{ source('wwi_oltp', 'SupplierCategories') }}),
     people as (select person_id, full_name from {{ source('wwi_oltp', 'People') }}),
supplier_enriched as (
    select
        s.wwi_supplier_id,
        s.supplier,
        s.postal_code,
        s.supplier_reference,
        s.payment_days,
        s.valid_from,
        s.valid_to,
        coalesce(sc.supplier_category_name, 'Unknown') as category,
        coalesce(p.full_name, '') as primary_contact
    from suppliers s
    left join categories sc on s.supplier_category_id = sc.supplier_category_id
        and sc.valid_from <= s.valid_from and (sc.valid_to is null or sc.valid_to > s.valid_from)
    left join people p on s.primary_contact_person_id = p.person_id
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