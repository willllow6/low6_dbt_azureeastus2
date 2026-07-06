with

source as (

    select *
    from {{ source('penn', 'soccer_players') }}

),

renamed as (

    select

        ---------- ids
        id as player_id,
        external_id,

        ---------- strings
        provider,
        first_name,
        last_name,
        position,
        birth_country,
        birth_city,
        footedness,

        ---------- numerics
        number,

        ---------- timestamps
        cast(last_seen_at as timestamp_ntz) as last_seen_at,
        cast(created_at as timestamp_ntz) as created_at,
        cast(updated_at as timestamp_ntz) as updated_at

    from source

)

select * from renamed
