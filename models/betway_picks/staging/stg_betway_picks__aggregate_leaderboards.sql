with

source as (

    select *
    from {{ source('betway_picks', 'aggregate_leaderboards') }}

),

renamed as (

    select

        ----------  ids
        id as aggregate_leaderboard_id,
        user_id,

        ---------- strings
        league_code,
        period_type,
        period_start,
        period_end,
        pickems_data as contest_data,
        contest_type as leaderboard_competition,

        ---------- numerics
        total_points,
        rank as leaderboard_position,
    
        ---------- booleans

        ---------- dates

        ---------- timestamps
        created_at,
        updated_at

    from source

)

select * from renamed