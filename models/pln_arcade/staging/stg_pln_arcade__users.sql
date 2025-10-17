with

source as (

    select *
    from {{ source('pln_arcade','users') }}

),

renamed as (

    select
        userid as user_id,
        deviceid as device_id,
        appleidsub as apple_id,
        email,
        username,
        birthdate as date_of_birth,
        case 
            when year(birthdate) = 1900 then 'Unknown' 
            when year(birthdate) < 1946 then 'Silent Generation' 
            when year(birthdate) < 1966 then 'Baby Boomer' 
            when year(birthdate) < 1980 then 'Generation X' 
            when year(birthdate) < 1996 then 'Millenials' 
            else 'Generation Z' 
        end as generation,
        case 
            when year(birthdate) = 1900 then null
            else floor(datediff('day', birthdate, current_date() )/365) 
        end as age,
        case
            when year(birthdate) = 1900 then 'Unknown' 
            when floor(datediff('day', birthdate, current_date() )/365) < 18 then '13-17'
            when floor(datediff('day', birthdate, current_date() )/365) < 21 then '18-20'
            when floor(datediff('day', birthdate, current_date() )/365) < 26 then '21-25'
            when floor(datediff('day', birthdate, current_date() )/365) < 31 then '26-30'
            when floor(datediff('day', birthdate, current_date() )/365) < 36 then '31-35'
            when floor(datediff('day', birthdate, current_date() )/365) < 41 then '36-40'
            when floor(datediff('day', birthdate, current_date() )/365) < 46 then '41-45'
            when floor(datediff('day', birthdate, current_date() )/365) < 51 then '46-50'
            when floor(datediff('day', birthdate, current_date() )/365) < 56 then '51-55'
            when floor(datediff('day', birthdate, current_date() )/365) < 61 then '56-60'
            else '60+'
        end as age_band,
        isenabled as is_enabled,
        case when contactpermissions = 1 then true else false end as has_consented_pln_marketing,
        lastloginutc as last_login_at,
        loginstreakstartutc as login_streak_started_at,
        cast(createdat as date) as user_created_date,
        cast(convert_timezone('UTC','America/New_York',createdat) as date) as user_created_date_et,
        createdat as user_created_at,
        convert_timezone('UTC','America/New_York',createdat) as user_created_at_et
    from source
)

select * from renamed