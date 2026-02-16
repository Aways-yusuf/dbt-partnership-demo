{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'Invoices') }}
),

renamed as (
    select
        cast(InvoiceID as int64) as invoice_id,
        cast(CustomerID as int64) as customer_id,
        cast(BillToCustomerID as int64) as bill_to_customer_id,
        cast(OrderID as int64) as order_id,
        cast(DeliveryMethodID as int64) as delivery_method_id,
        cast(ContactPersonID as int64) as contact_person_id,
        cast(AccountsPersonID as int64) as accounts_person_id,
        cast(SalespersonPersonID as int64) as salesperson_person_id,
        cast(PackedByPersonID as int64) as packed_by_person_id,
        cast(InvoiceDate as date) as invoice_date,
        safe_cast(CustomerPurchaseOrderNumber as int64) as customer_purchase_order_number,
        cast(IsCreditNote as int64) as is_credit_note,
        CreditNoteReason as credit_note_reason,
        Comments as comments,
        DeliveryInstructions as delivery_instructions,
        InternalComments as internal_comments,
        cast(TotalDryItems as int64) as total_dry_items,
        cast(TotalChillerItems as int64) as total_chiller_items,
        DeliveryRun as delivery_run,
        RunPosition as run_position,
        ReturnedDeliveryData as returned_delivery_data,
        {{ parse_timestamp('ConfirmedDeliveryTime') }} as confirmed_delivery_time,
        ConfirmedReceivedBy as confirmed_received_by,
        cast(LastEditedBy as int64) as last_edited_by,
        {{ parse_timestamp('LastEditedWhen') }} as last_edited_when
    from source
    where InvoiceID is not null
)

select * from renamed

