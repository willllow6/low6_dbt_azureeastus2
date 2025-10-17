with

source as (

    select *
    from {{ source('betway_picks', 'users') }}

),

renamed as (

    select

        ----------  ids
        id as user_id,
        -- user_id,
        service_user_id,
        sso_user_id,
        -- usertoken as user_token,

        ---------- strings
        username,
        email,
        -- favourite_team_id
        country,
        state,
        '' as location,
        -- referral_code,

        ---------- numerics

        ---------- booleans

        ---------- dates
        cast(created_at as date) as registration_date,
        cast(convert_timezone('UTC','America/New_York',created_at) as date) as registration_date_et,

        ---------- timestamps
        created_at as registered_at,
        convert_timezone('UTC','America/New_York',created_at) as registered_at_et

    from source

)

select * from renamed
