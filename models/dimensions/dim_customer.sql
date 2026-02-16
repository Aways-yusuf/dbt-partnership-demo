{{ config(
    materialized='incremental',
    unique_key='customer_key',
    on_schema_change='fail'
) }}

{% if is_incremental() %}
    {% set end_of_time = "timestamp('9999-12-31 23:59:59.999999')" %}
{% else %}
    {% set end_of_time = "timestamp('9999-12-31 23:59:59.999999')" %}
{% endif %}

with source_data as (
    select * from {{ ref('int_customer_enriched') }}
),

{% if is_incremental() %}
all_existing_rows as (
    select * from {{ this }}
),
existing_current_rows as (
    select * from {{ this }}
    where is_current = true
),
{% endif %}

new_rows as (
    select
        {{ surrogate_key(['wwi_customer_id']) }} as customer_key,
        cast(wwi_customer_id as int64) as wwi_customer_id,
        customer,
        bill_to_customer,
        category,
        buying_group,
        primary_contact,
        postal_code,
        valid_from,
        coalesce(valid_to, {{ end_of_time }}) as valid_to,
        {{ hash_diff(['customer', 'bill_to_customer', 'category', 'buying_group', 'primary_contact', 'postal_code']) }} as hashdiff
    from source_data
    where wwi_customer_id is not null
),

{% if is_incremental() %}
rows_to_close as (
    select
        er.customer_key,
        er.wwi_customer_id,
        min(nr.valid_from) as new_valid_from
    from existing_current_rows er
    inner join new_rows nr
        on er.wwi_customer_id = nr.wwi_customer_id
    group by er.customer_key, er.wwi_customer_id
),

closed_rows as (
    select
        er.*,
        rtc.new_valid_from as valid_to_updated,
        case
            when rtc.new_valid_from is not null then false
            else er.is_current
        end as is_current_updated
    from existing_current_rows er
    left join rows_to_close rtc
        on er.customer_key = rtc.customer_key
),

historical_rows as (
    select
        customer_key,
        wwi_customer_id,
        customer,
        bill_to_customer,
        category,
        buying_group,
        primary_contact,
        postal_code,
        valid_from,
        valid_to,
        hashdiff,
        is_current
    from all_existing_rows
    where is_current = false
),

final as (
    -- Historical rows (unchanged)
    select * from historical_rows
    
    union all
    
    -- Updated current rows (now closed)
    select
        customer_key,
        wwi_customer_id,
        customer,
        bill_to_customer,
        category,
        buying_group,
        primary_contact,
        postal_code,
        valid_from,
        coalesce(valid_to_updated, valid_to) as valid_to,
        hashdiff,
        is_current_updated as is_current
    from closed_rows
    
    union all
    
    -- New rows
    select
        customer_key,
        wwi_customer_id,
        customer,
        bill_to_customer,
        category,
        buying_group,
        primary_contact,
        postal_code,
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
        customer_key,
        wwi_customer_id,
        customer,
        bill_to_customer,
        category,
        buying_group,
        primary_contact,
        postal_code,
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

