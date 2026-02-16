{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'Suppliers') }}
),

renamed as (
    select
        cast(SupplierID as int64) as wwi_supplier_id,
        SupplierName as supplier_name,
        cast(SupplierCategoryID as int64) as supplier_category_id,
        cast(PrimaryContactPersonID as int64) as primary_contact_person_id,
        cast(AlternateContactPersonID as int64) as alternate_contact_person_id,
        safe_cast(DeliveryMethodID as int64) as delivery_method_id,
        cast(DeliveryCityID as int64) as delivery_city_id,
        cast(PostalCityID as int64) as postal_city_id,
        SupplierReference as supplier_reference,
        BankAccountName as bank_account_name,
        BankAccountBranch as bank_account_branch,
        cast(BankAccountCode as int64) as bank_account_code,
        cast(BankAccountNumber as int64) as bank_account_number,
        cast(BankInternationalCode as int64) as bank_international_code,
        cast(PaymentDays as int64) as payment_days,
        InternalComments as internal_comments,
        PhoneNumber as phone_number,
        FaxNumber as fax_number,
        WebsiteURL as website_url,
        DeliveryAddressLine1 as delivery_address_line1,
        DeliveryAddressLine2 as delivery_address_line2,
        cast(DeliveryPostalCode as int64) as delivery_postal_code,
        cast(DeliveryLocation as float64) as delivery_location,
        PostalAddressLine1 as postal_address_line1,
        PostalAddressLine2 as postal_address_line2,
        cast(PostalPostalCode as int64) as postal_postal_code,
        {{ parse_timestamp('ValidFrom') }} as valid_from,
        {{ parse_timestamp('ValidTo') }} as valid_to,
        cast(LastEditedBy as int64) as last_edited_by
    from source
    where SupplierID is not null
)

select * from renamed

