with

source as (

    select *
    from {{ source('saracen_bracket', 'users') }}
    where userid > 1
    
),

renamed as (

    select

        ----------  ids
        userid as user_id,
        ssouserid as sso_user_id,
        accountid as account_id,

        ---------- strings
        firstname as first_name,
        lastname as last_name,
        email,
        username,
        country as country_code,
        case 
            when year(dateofbirth) = 1900 then null
            else floor(datediff('day', dateofbirth, current_date() )/365) 
        end as user_age,
        case
            when user_age is null then 'Unknown' 
            when user_age< 18 then '13-17'
            when user_age< 21 then '18-20'
            when user_age< 26 then '21-25'
            when user_age< 31 then '26-30'
            when user_age< 36 then '31-35'
            when user_age< 41 then '36-40'
            when user_age< 46 then '41-45'
            when user_age< 51 then '46-50'
            when user_age< 56 then '51-55'
            when user_age< 61 then '56-60'
            else '60+'
        end as user_age_band,
        case 
            when year(dateofbirth) = 1900 then 'Unknown' 
            when year(dateofbirth) < 1946 then 'Silent Generation' 
            when year(dateofbirth) < 1966 then 'Baby Boomer' 
            when year(dateofbirth) < 1980 then 'Generation X' 
            when year(dateofbirth) < 1996 then 'Millenials' 
            else 'Generation Z' 
        end as user_generation,

        ---------- numerics

        ---------- booleans

        ---------- dates
        cast(createdat as date) as created_date,
        cast(convert_timezone('UTC','America/New_York',createdat::timestamp_ntz) as date) as created_date_et,
        dateofbirth as date_of_birth,

        ---------- timestamps
        createdat as created_at,
        convert_timezone('UTC','America/New_York',created_at::timestamp_ntz) as created_at_et

    from source


)

select * from renamed