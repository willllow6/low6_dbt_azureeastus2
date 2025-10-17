with

source as (

    select *
    from {{ source('betway_picks', 'pickem_leaderboards') }}

),

renamed as (

    select

        ----------  ids
        id as contest_leaderboard_id,
        user_id,
        pickem_id as contest_id,

        ---------- strings

        ---------- numerics
        points,
        rank as leaderboard_position,
        tiebreaker_margin,

        ---------- booleans

        ---------- dates

        ---------- timestamps
        created_at,
        updated_at
        

    from source

)

select * from renamed
