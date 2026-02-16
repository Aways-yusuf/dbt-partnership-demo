{{ config(
    materialized='incremental',
    unique_key='transaction_type_key',
    on_schema_change='fail'
) }}

{% if is_incremental() %}
    {% set end_of_time = "timestamp('9999-12-31 23:59:59.999999')" %}
{% else %}
    {% set end_of_time = "timestamp('9999-12-31 23:59:59.999999')" %}
{% endif %}

with source_data as (
    select * from {{ ref('stg_transaction_types') }}
),

{% if is_incremental() %}
existing_rows as (
    select * from {{ this }}
    where is_current = true
),
{% endif %}

new_rows as (
    select
        {{ surrogate_key(['wwi_transaction_type_id']) }} as transaction_type_key,
        cast(wwi_transaction_type_id as int64) as wwi_transaction_type_id,
        transaction_type_name as transaction_type,
        valid_from,
        coalesce(valid_to, {{ end_of_time }}) as valid_to,
        {{ hash_diff(['transaction_type_name']) }} as hashdiff
    from source_data
    where wwi_transaction_type_id is not null
),

{% if is_incremental() %}
rows_to_close as (
    select
        er.transaction_type_key,
        er.wwi_transaction_type_id,
        min(nr.valid_from) as new_valid_from
    from existing_rows er
    inner join new_rows nr
        on er.wwi_transaction_type_id = nr.wwi_transaction_type_id
    group by er.transaction_type_key, er.wwi_transaction_type_id
),

closed_rows as (
    select
        er.*,
        rtc.new_valid_from as valid_to_updated,
        case
            when rtc.new_valid_from is not null then false
            else er.is_current
        end as is_current_updated
    from existing_rows er
    left join rows_to_close rtc
        on er.transaction_type_key = rtc.transaction_type_key
),

final as (
    select
        transaction_type_key,
        wwi_transaction_type_id,
        transaction_type,
        valid_from,
        coalesce(valid_to_updated, valid_to) as valid_to,
        hashdiff,
        is_current_updated as is_current
    from closed_rows
    
    union all
    
    select
        transaction_type_key,
        wwi_transaction_type_id,
        transaction_type,
        valid_from,
        valid_to,
        hashdiff,
        case
            when valid_to = {{ end_of_time }} then true
            else false
        end as is_current
    from new_rows
)
{% else %}
final as (
    select
        transaction_type_key,
        wwi_transaction_type_id,
        transaction_type,
        valid_from,
        valid_to,
        hashdiff,
        case
            when valid_to = {{ end_of_time }} then true
            else false
        end as is_current
    from new_rows
)
{% endif %}

select * from final

