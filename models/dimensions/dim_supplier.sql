-- Dimension Supplier (SCD2). Replaces MigrateStagedSupplierData → Dimension.Supplier.
{{ config(materialized='table', schema='dimensions') }}
with s as (select * from {{ ref('stg_purchasing__suppliers') }}),
with_valid_to as (
    select wwi_supplier_id, supplier, postal_code, valid_from,
           coalesce(lead(valid_from) over (partition by wwi_supplier_id order by valid_from), timestamp('9999-12-31 23:59:59.999999')) as valid_to
    from s
)
select row_number() over (order by wwi_supplier_id, valid_from) as supplier_key,
       wwi_supplier_id, supplier, postal_code, valid_from, valid_to
from with_valid_to