{{ config(
    materialized='incremental',
    unique_key='movement_key',
    partition_by={'field': 'date_key', 'data_type': 'date'},
    cluster_by=['stock_item_key', 'transaction_type_key']
) }}

{% if is_incremental() %}
    {% set cutoff_date = "date_sub(current_date(), interval 7 day)" %}
{% endif %}

with stock_item_transactions as (
    select * from {{ ref('stg_stock_item_transactions') }}
    {% if is_incremental() %}
    where last_edited_when >= timestamp({{ cutoff_date }})
    {% endif %}
),

dim_stock_item as (
    select * from {{ ref('dim_stock_item') }}
),

dim_transaction_type as (
    select * from {{ ref('dim_transaction_type') }}
),

movement_with_keys as (
    select
        sit.*,
        coalesce((
            select stock_item_key
            from dim_stock_item dsi
            where dsi.wwi_stock_item_id = sit.stock_item_id
                and sit.last_edited_when > dsi.valid_from
                and sit.last_edited_when <= dsi.valid_to
            order by dsi.valid_from
            limit 1
        ), {{ surrogate_key(['sit.stock_item_id']) }}) as stock_item_key,
        coalesce((
            select transaction_type_key
            from dim_transaction_type dtt
            where dtt.wwi_transaction_type_id = sit.transaction_type_id
                and sit.last_edited_when > dtt.valid_from
                and sit.last_edited_when <= dtt.valid_to
            order by dtt.valid_from
            limit 1
        ), {{ surrogate_key(['sit.transaction_type_id']) }}) as transaction_type_key
    from stock_item_transactions sit
),

final as (
    select
        {{ surrogate_key(['stock_item_transaction_id']) }} as movement_key,
        stock_item_key,
        transaction_type_key,
        cast(transaction_occurred_when as date) as date_key,
        cast(stock_item_transaction_id as int64) as wwi_stock_item_transaction_id,
        safe_cast(customer_id as int64) as wwi_customer_id,
        safe_cast(invoice_id as int64) as wwi_invoice_id,
        safe_cast(supplier_id as int64) as wwi_supplier_id,
        safe_cast(purchase_order_id as int64) as wwi_purchase_order_id,
        cast(quantity as float64) as quantity
    from movement_with_keys
)

{% if is_incremental() %}
select * from final
where not exists (
    select 1
    from {{ this }}
    where {{ this }}.movement_key = final.movement_key
)
{% else %}
select * from final
{% endif %}

