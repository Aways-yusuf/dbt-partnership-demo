{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'Customers') }}
),

renamed as (
    select
        cast(CustomerID as int64) as wwi_customer_id,
        CustomerName as customer_name,
        cast(BillToCustomerID as int64) as bill_to_customer_id,
        cast(CustomerCategoryID as int64) as customer_category_id,
        safe_cast(BuyingGroupID as int64) as buying_group_id,
        cast(PrimaryContactPersonID as int64) as primary_contact_person_id,
        safe_cast(AlternateContactPersonID as int64) as alternate_contact_person_id,
        cast(DeliveryMethodID as int64) as delivery_method_id,
        cast(DeliveryCityID as int64) as delivery_city_id,
        cast(PostalCityID as int64) as postal_city_id,
        safe_cast(CreditLimit as float64) as credit_limit,
        cast(AccountOpenedDate as date) as account_opened_date,
        cast(StandardDiscountPercentage as float64) as standard_discount_percentage,
        cast(IsStatementSent as int64) as is_statement_sent,
        cast(IsOnCreditHold as int64) as is_on_credit_hold,
        cast(PaymentDays as int64) as payment_days,
        PhoneNumber as phone_number,
        FaxNumber as fax_number,
        DeliveryRun as delivery_run,
        RunPosition as run_position,
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
    where CustomerID is not null
)

select * from renamed

