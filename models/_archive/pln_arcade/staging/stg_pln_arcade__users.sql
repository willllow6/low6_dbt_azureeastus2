with

source as (

    select *
    from {{ source('pln_arcade','users') }}

),

renamed as (

    select
        user_id,
        device_id,
        apple_id_sub as apple_id,
        email,
        username,
        birth_date as date_of_birth,
        case 
            when year(birth_date) = 1900 or birth_date is null then 'Unknown' 
            when year(birth_date) < 1946 then 'Silent Generation' 
            when year(birth_date) < 1966 then 'Baby Boomer' 
            when year(birth_date) < 1980 then 'Generation X' 
            when year(birth_date) < 1996 then 'Millenials' 
            else 'Generation Z' 
        end as generation,
        case 
            when year(birth_date) = 1900 then null
            else floor(datediff('day', birth_date, current_date() )/365) 
        end as age,
        case
            when year(birth_date) = 1900 or birth_date is null then 'Unknown' 
            when floor(datediff('day', birth_date, current_date() )/365) < 18 then '13-17'
            when floor(datediff('day', birth_date, current_date() )/365) < 21 then '18-20'
            when floor(datediff('day', birth_date, current_date() )/365) < 26 then '21-25'
            when floor(datediff('day', birth_date, current_date() )/365) < 31 then '26-30'
            when floor(datediff('day', birth_date, current_date() )/365) < 36 then '31-35'
            when floor(datediff('day', birth_date, current_date() )/365) < 41 then '36-40'
            when floor(datediff('day', birth_date, current_date() )/365) < 46 then '41-45'
            when floor(datediff('day', birth_date, current_date() )/365) < 51 then '46-50'
            when floor(datediff('day', birth_date, current_date() )/365) < 56 then '51-55'
            when floor(datediff('day', birth_date, current_date() )/365) < 61 then '56-60'
            else '60+'
        end as age_band,
        is_enabled as is_enabled,
        contact_permissions as has_consented_pln_marketing,
        last_login_utc as last_login_at,
        login_streak_start_utc as login_streak_started_at,
        cast(created_at as date) as user_created_date,
        cast(convert_timezone('UTC','America/New_York',created_at) as date) as user_created_date_et,
        created_at as user_created_at,
        convert_timezone('UTC','America/New_York',created_at) as user_created_at_et
    from source
)

select * from renamed