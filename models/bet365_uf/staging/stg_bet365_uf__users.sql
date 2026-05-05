with

source as (

    select *
    from {{ source('bet365_uf', 'USERS') }}

),

renamed as (

    select

        ----------  ids
        UserId as user_id,
        'bet365' as client_id,

        ---------- strings
        Email as email,
        Username as username,
        FirstName as first_name,
        LastName as last_name,
        parse_json(Address):state::text as user_state,

        case
            when year(BirthDate) = 1900 then null
            else floor(datediff('day', BirthDate, current_date()) / 365)
        end as user_age,

        case
            when (case when year(BirthDate) = 1900 then null else floor(datediff('day', BirthDate, current_date()) / 365) end) is null then 'Unknown'
            when (case when year(BirthDate) = 1900 then null else floor(datediff('day', BirthDate, current_date()) / 365) end) < 18 then '13-17'
            when (case when year(BirthDate) = 1900 then null else floor(datediff('day', BirthDate, current_date()) / 365) end) < 21 then '18-20'
            when (case when year(BirthDate) = 1900 then null else floor(datediff('day', BirthDate, current_date()) / 365) end) < 26 then '21-25'
            when (case when year(BirthDate) = 1900 then null else floor(datediff('day', BirthDate, current_date()) / 365) end) < 31 then '26-30'
            when (case when year(BirthDate) = 1900 then null else floor(datediff('day', BirthDate, current_date()) / 365) end) < 36 then '31-35'
            when (case when year(BirthDate) = 1900 then null else floor(datediff('day', BirthDate, current_date()) / 365) end) < 41 then '36-40'
            when (case when year(BirthDate) = 1900 then null else floor(datediff('day', BirthDate, current_date()) / 365) end) < 46 then '41-45'
            when (case when year(BirthDate) = 1900 then null else floor(datediff('day', BirthDate, current_date()) / 365) end) < 51 then '46-50'
            when (case when year(BirthDate) = 1900 then null else floor(datediff('day', BirthDate, current_date()) / 365) end) < 56 then '51-55'
            when (case when year(BirthDate) = 1900 then null else floor(datediff('day', BirthDate, current_date()) / 365) end) < 61 then '56-60'
            else '60+'
        end as user_age_band,

        case
            when year(BirthDate) = 1900 then 'Unknown'
            when year(BirthDate) < 1946 then 'Silent Generation'
            when year(BirthDate) < 1966 then 'Baby Boomer'
            when year(BirthDate) < 1980 then 'Generation X'
            when year(BirthDate) < 1996 then 'Millenials'
            else 'Generation Z'
        end as user_generation,

        'form' as registration_type,
        'fantasy' as game_type,

        ---------- numerics
        MobileNumber as mobile_number,

        ---------- booleans
        email like 'deleted_%' as is_deleted,
        IsEnabled as is_enabled,

        case
            when IsEnabled and not (email like 'deleted_%')
                then true
            else false
        end as is_playable,

        IsTester as is_tester,

        parse_json(ContactPermissions):optin1Accepted::boolean as has_agreed_privacy_policy,
        parse_json(ContactPermissions):optin2Accepted::boolean as has_accepted_terms_and_conditions,
        parse_json(ContactPermissions):optin3Accepted::boolean as has_confirmed_18_plus,
        parse_json(ContactPermissions):optin4Accepted::boolean as has_consented_marketing,
        IsProfileComplete as has_completed_profile,

        ---------- dates
        date(BirthDate) as birth_date,
        cast(CreatedAt as date) as registered_date,

        ---------- timestamps
        LastLoginUtc as last_login_at,
        CreatedAt as registered_at

    from source

)

select * from renamed
