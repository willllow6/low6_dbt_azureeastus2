with

source as (

    select *
    from {{ source('bet99_bracket', 'teams') }}

),

renamed as (

    select

        ----------  ids
        teamid as team_id,

        ---------- strings
        '{{ var("bet99_bracket_current_tournament") }}' as tournament_name,
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
