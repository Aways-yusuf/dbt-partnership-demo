{{ config(materialized='view') }}

with people as (
    select * from {{ ref('stg_people') }}
)

select
    p.wwi_person_id as wwi_employee_id,
    p.full_name as employee,
    p.preferred_name,
    p.is_salesperson,
    p.valid_from,
    p.valid_to,
    p.last_edited_by
from people p
where p.is_employee = 1

