with

source as (

    select *
    from {{ source('saracen_bracket_nbafinals2026', 'teams') }}

),

renamed as (

    select

        ----------  ids
        teamid as team_id,

        ---------- strings
        'nba_finals_2026' as tournament_name,
        name as team_name,
        competition as contest_name,
        rank as team_rank,
        possiblepoints as possible_points,
        winpercentage as win_percentage,

        ---------- numerics

        ---------- booleans

        ---------- dates
        cast(createdat as date) as created_date,

        ---------- timestamps
        createdat as created_at

    from source

)

select * from renamed
