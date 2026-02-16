{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'Colors') }}
),

renamed as (
    select
        cast(ColorID as int64) as color_id,
        ColorName as color_name,
        {{ parse_timestamp('ValidFrom') }} as valid_from,
        {{ parse_timestamp('ValidTo') }} as valid_to,
        cast(LastEditedBy as int64) as last_edited_by
    from source
    where ColorID is not null
)

select * from renamed

