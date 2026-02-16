{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'People') }}
),

renamed as (
    select
        cast(PersonID as int64) as wwi_person_id,
        FullName as full_name,
        PreferredName as preferred_name,
        SearchName as search_name,
        cast(IsPermittedToLogon as int64) as is_permitted_to_logon,
        LogonName as logon_name,
        cast(IsExternalLogonProvider as int64) as is_external_logon_provider,
        HashedPassword as hashed_password,
        cast(IsSystemUser as int64) as is_system_user,
        cast(IsEmployee as int64) as is_employee,
        cast(IsSalesperson as int64) as is_salesperson,
        UserPreferences as user_preferences,
        PhoneNumber as phone_number,
        FaxNumber as fax_number,
        EmailAddress as email_address,
        Photo as photo,
        CustomFields as custom_fields,
        OtherLanguages as other_languages,
        {{ parse_timestamp('ValidFrom') }} as valid_from,
        {{ parse_timestamp('ValidTo') }} as valid_to,
        cast(LastEditedBy as int64) as last_edited_by
    from source
    where PersonID is not null
)

select * from renamed

