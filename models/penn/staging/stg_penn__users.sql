with

source as (

    select *
    from {{ source('penn', 'users') }}

),

renamed as (

    select

        ---------- ids
        id as user_id,
        penn_user_id,

        ---------- strings
        platform,
        'sso' as registration_type,

        ---------- numerics
        tier,

        ---------- dates
        cast(cast(created_at as timestamp_ntz) as date) as registration_date,

        ---------- timestamps
        cast(created_at as timestamp_ntz) as registered_at,
        cast(updated_at as timestamp_ntz) as updated_at

    from source

)

select * from renamed
