with

source as (

    select * 
    from {{ source('bet365_overunder','users') }}

),

renamed as (

    select

        id as user_id,
        gaming_id,
       
        currency as currency_code,
        country_code,
        state_code,
        segment_group,

        case when last_login_at >= '2025-09-04 07:00:00' then true else false end as has_logged_in_since_launch,

        cast(created_at as date) as registration_date,
        cast(convert_timezone('UTC','America/New_York',to_timestamp_ntz(created_at)) as date) as registration_date_et,

        to_timestamp_ntz(created_at) as registered_at,
        convert_timezone('UTC','America/New_York',to_timestamp_ntz(created_at)) as registered_at_et,
        to_timestamp_ntz(updated_at) as updated_at,
        to_timestamp_ntz(last_login_at) as last_login_at
        
    from source

)

select * from renamed