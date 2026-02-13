-- Staging: Application.People (source for Dimension.Employee). Replaces GetEmployeeUpdates → Employee_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'people') }}
),
renamed as (
    select
        person_id as wwi_employee_id,
        full_name as employee,
        preferred_name,
        is_salesperson,
        valid_from,
        valid_to
    from source
)
select * from renamed