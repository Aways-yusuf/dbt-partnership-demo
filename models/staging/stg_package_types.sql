{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'PackageTypes') }}
),

renamed as (
    select
        cast(PackageTypeID as int64) as package_type_id,
        PackageTypeName as package_type_name,
        {{ parse_timestamp('ValidFrom') }} as valid_from,
        {{ parse_timestamp('ValidTo') }} as valid_to,
        cast(LastEditedBy as int64) as last_edited_by
    from source
    where PackageTypeID is not null
)

select * from renamed

