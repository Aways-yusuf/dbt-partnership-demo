{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'InvoiceLines') }}
),

renamed as (
    select
        cast(InvoiceLineID as int64) as invoice_line_id,
        cast(InvoiceID as int64) as invoice_id,
        cast(StockItemID as int64) as stock_item_id,
        Description as description,
        cast(PackageTypeID as int64) as package_type_id,
        cast(Quantity as int64) as quantity,
        cast(UnitPrice as float64) as unit_price,
        cast(TaxRate as float64) as tax_rate,
        cast(TaxAmount as float64) as tax_amount,
        cast(LineProfit as float64) as line_profit,
        cast(ExtendedPrice as float64) as extended_price,
        cast(LastEditedBy as int64) as last_edited_by,
        {{ parse_timestamp('LastEditedWhen') }} as last_edited_when
    from source
    where InvoiceLineID is not null
        and InvoiceID is not null
        and StockItemID is not null
)

select * from renamed

