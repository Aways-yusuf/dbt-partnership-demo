{{ config(
    materialized='incremental',
    unique_key='transaction_key',
    partition_by={'field': 'date_key', 'data_type': 'date'},
    cluster_by=['customer_key', 'supplier_key', 'transaction_type_key']
) }}

{% if is_incremental() %}
    {% set cutoff_date = "date_sub(current_date(), interval 7 day)" %}
{% endif %}

with transaction_prep as (
    select * from {{ ref('int_transaction_prep') }}
    {% if is_incremental() %}
    where last_edited_when >= timestamp({{ cutoff_date }})
    {% endif %}
),

dim_customer as (
    select * from {{ ref('dim_customer') }}
),

dim_supplier as (
    select * from {{ ref('dim_supplier') }}
),

dim_transaction_type as (
    select * from {{ ref('dim_transaction_type') }}
),

dim_payment_method as (
    select * from {{ ref('dim_payment_method') }}
),

transaction_with_keys as (
    select
        tp.*,
        coalesce((
            select customer_key
            from dim_customer dc
            where dc.wwi_customer_id = tp.wwi_customer_id
                and tp.last_edited_when > dc.valid_from
                and tp.last_edited_when <= dc.valid_to
            order by dc.valid_from
            limit 1
        ), null) as customer_key,
        coalesce((
            select customer_key
            from dim_customer dc
            where dc.wwi_customer_id = tp.wwi_bill_to_customer_id
                and tp.last_edited_when > dc.valid_from
                and tp.last_edited_when <= dc.valid_to
            order by dc.valid_from
            limit 1
        ), null) as bill_to_customer_key,
        coalesce((
            select supplier_key
            from dim_supplier ds
            where ds.wwi_supplier_id = tp.wwi_supplier_id
                and tp.last_edited_when > ds.valid_from
                and tp.last_edited_when <= ds.valid_to
            order by ds.valid_from
            limit 1
        ), null) as supplier_key,
        coalesce((
            select transaction_type_key
            from dim_transaction_type dtt
            where dtt.wwi_transaction_type_id = tp.wwi_transaction_type_id
                and tp.last_edited_when > dtt.valid_from
                and tp.last_edited_when <= dtt.valid_to
            order by dtt.valid_from
            limit 1
        ), {{ surrogate_key(['tp.wwi_transaction_type_id']) }}) as transaction_type_key,
        coalesce((
            select payment_method_key
            from dim_payment_method dpm
            where dpm.wwi_payment_method_id = tp.wwi_payment_method_id
                and tp.last_edited_when > dpm.valid_from
                and tp.last_edited_when <= dpm.valid_to
            order by dpm.valid_from
            limit 1
        ), null) as payment_method_key
    from transaction_prep tp
),

transaction_keys_prep as (
    select
        twk.*,
        coalesce(cast(twk.wwi_customer_id as string), '') as customer_id_str,
        coalesce(cast(twk.wwi_supplier_id as string), '') as supplier_id_str,
        cast(twk.transaction_date as string) as transaction_date_str,
        coalesce(cast(twk.wwi_invoice_id as string), '') as invoice_id_str,
        coalesce(cast(twk.wwi_purchase_order_id as string), '') as purchase_order_id_str
    from transaction_with_keys twk
),

final as (
    select
        {{ surrogate_key(['customer_id_str', 'supplier_id_str', 'transaction_date_str', 'invoice_id_str', 'purchase_order_id_str']) }} as transaction_key,
        cast(transaction_date as date) as date_key,
        customer_key,
        bill_to_customer_key,
        supplier_key,
        transaction_type_key,
        payment_method_key,
        safe_cast(wwi_customer_id as int64) as wwi_customer_transaction_id,
        safe_cast(wwi_supplier_id as int64) as wwi_supplier_transaction_id,
        safe_cast(wwi_invoice_id as int64) as wwi_invoice_id,
        safe_cast(wwi_purchase_order_id as int64) as wwi_purchase_order_id,
        supplier_invoice_number,
        cast(amount_excluding_tax as float64) as total_excluding_tax,
        cast(tax_amount as float64) as tax_amount,
        cast(transaction_amount as float64) as total_including_tax,
        cast(outstanding_balance as float64) as outstanding_balance,
        cast(is_finalized as int64) as is_finalized
    from transaction_keys_prep
)

{% if is_incremental() %}
select * from final
where not exists (
    select 1
    from {{ this }}
    where {{ this }}.transaction_key = final.transaction_key
)
{% else %}
select * from final
{% endif %}

