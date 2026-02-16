-- Dimension Supplier (SCD Type 2). Replaces MigrateStagedSupplierData → Dimension.Supplier.
-- Dependency: Stock Item dimension (load order; build Stock Item before Supplier when using same run).
-- Columns align with Integration.Supplier_Staging / Dimension.Supplier.
{{ config(materialized='table', schema='dimensions') }}
with supplier_joined as (select * from {{ ref('int_supplier__joined') }})
select
    row_number() over (order by wwi_supplier_id, valid_from) as supplier_key,
    wwi_supplier_id,
    supplier,
    category,
    primary_contact,
    supplier_reference,
    payment_days,
    postal_code,
    valid_from,
    valid_to
from supplier_joined