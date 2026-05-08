with

source as (

    select *
    from {{ source('penn', 'users') }}

),

renamed as (

    select

        ---------- ids
        id as user_id,
        sso_user_id,

        ---------- strings
        username,
        -- email,
        -- favourite_team_id,
        -- country,
        -- state,
        -- location,
        -- referral_code,

        ---------- derived
        case
            when sso_user_id is not null then 'sso'
            else 'form'
        end as registration_type,

        ---------- dates
        cast(created_at as date) as registration_date,

        ---------- timestamps
        created_at as registered_at

    from source

)

select * from renamed
