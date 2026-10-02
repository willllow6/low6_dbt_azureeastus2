with

source as (

    select *
    from {{ source('oilers_picks', 'leaguetournaments') }}

),

renamed as (

    select

        ----------  ids
        leaguetournamentid::varchar as period_leaderboard_id,
        leagueid::varchar as friend_league_id,
        'oilers' as client_id,
        'oilers' as tenant_id,

        ---------- strings
        'Edmonton Oilers' as tenant_name,
        'pickem' as game_type,
        title as period_leaderboard_name,
        tournamenttype as period_type,

        ---------- booleans
        isvisible as is_visible,

        ---------- timestamps
        createdat as created_at,
        startutc as period_start,
        endutc as period_end

    from source

)

select * from renamed
