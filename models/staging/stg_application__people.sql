-- Staging: Application.People (source for Dimension.Employee). Replaces GetEmployeeUpdates → Employee_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'People') }}
),
renamed as (
    select
        personid as wwi_employee_id,
        fullname as employee,
        preferredname,
        issalesperson,
        validfrom,
        validto
    from source
)
select * from renamed