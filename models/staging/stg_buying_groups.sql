{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'BuyingGroups') }}
),

renamed as (
    select
        cast(BuyingGroupID as int64) as buying_group_id,
        BuyingGroupName as buying_group_name,
        {{ parse_timestamp('ValidFrom') }} as valid_from,
        {{ parse_timestamp('ValidTo') }} as valid_to,
        cast(LastEditedBy as int64) as last_edited_by
    from source
    where BuyingGroupID is not null
)

select * from renamed

