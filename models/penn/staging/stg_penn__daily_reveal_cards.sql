with

source as (

    select *
    from {{ source('penn', 'daily_reveal_cards') }}

),

renamed as (

    select

        ---------- ids
        id as card_id,
        external_id,
        team_external_id,

        ---------- strings
        display_name,
        first_name,
        last_name,
        position,
        team_name,
        team_abbreviation,
        jersey_number,
        rating,
        injury_status,
        sport,
        photo_url,
        batting_average,

        ---------- booleans
        is_active,

        ---------- timestamps
        created_at,
        updated_at

    from source

)

select * from renamed
