-- Dimension Employee (SCD2). Replaces MigrateStagedEmployeeData → Dimension.Employee.
{{ config(materialized='table', schema='dimensions') }}
with people as (select * from {{ ref('stg_application__people') }}),
with_valid_to as (
    select wwi_employee_id, employee, preferred_name, valid_from,
           coalesce(lead(valid_from) over (partition by wwi_employee_id order by valid_from), timestamp('9999-12-31 23:59:59.999999')) as valid_to
    from people
)
select row_number() over (order by wwi_employee_id, valid_from) as employee_key,
       wwi_employee_id, employee, preferred_name, valid_from, valid_to
from with_valid_to