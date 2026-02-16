-- Intermediate: Supplier + Category + Primary Contact (point-in-time).
-- Replaces GetSupplierUpdates logic: SCD Type 2 with Category and Primary Contact from lookups.
-- Dependency: Stock Item (load order only; Supplier runs after Stock Item in SSIS).
{{ config(materialized='view', schema='intermediate') }}
with suppliers as (select * from {{ ref('stg_purchasing__suppliers') }}),
     categories as (
         select safe_cast(suppliercategoryid as int64) as suppliercategoryid, suppliercategoryname ,
                cast(validfrom as timestamp) as validfrom, cast(validto as timestamp) as validto
         from {{ source('wwi_oltp', 'SupplierCategories') }}
     ),
     people as (
         select safe_cast(personid as int64) as personid, fullname as fullname
         from {{ source('wwi_oltp', 'People') }}
     ),
supplier_enriched as (
    select
        s.wwi_supplier_id,
        s.supplier,
        s.postal_code,
        s.supplier_reference,
        s.payment_days,
        s.validfrom,
        s.validto,
        coalesce(sc.suppliercategoryname, 'Unknown') as category,
        coalesce(p.fullname, '') as primary_contact
    from suppliers s
    left join categories sc on s.suppliercategoryid = sc.suppliercategoryid
        and sc.validfrom <= s.validfrom and (sc.validto is null or sc.validto > s.validfrom)
    left join people p on s.primarycontactpersonid = p.personid
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
        validfrom as valid_from,
        coalesce(lead(validfrom) over (partition by wwi_supplier_id order by validfrom), timestamp('9999-12-31 23:59:59.999999')) as valid_to
    from supplier_enriched
)
select * from with_valid_to