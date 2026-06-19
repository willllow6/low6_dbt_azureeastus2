with

source as (

    select *
    from {{ source('saracen_bracket_nbafinals2026', 'matches') }}

),

renamed as (

    select

        ----------  ids
        matchid as match_id,
        conferenceid as conference_id,
        roundid as round_id,
        hometeamid as home_team_id,
        awayteamid as away_team_id,
        winnerteamid as winner_team_id,

        ---------- strings
        'nba_finals_2026' as tournament_name,
        slot as match_slot,
        result,
        competition as contest_name,

        ---------- numerics

        ---------- booleans
        isscore as is_scored,

        ---------- dates
        cast(createdat as date) as created_date,

        ---------- timestamps
        createdat as created_at,
        starttimeutc as match_starts_at

    from source

)

select * from renamed
