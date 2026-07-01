with

source as (

    select *
    from {{ source('bet99_bracket', 'matches') }}

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
        '{{ var("bet99_bracket_current_tournament") }}' as tournament_name,
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
