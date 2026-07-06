with

source as (

    select *
    from {{ source('oilers_picks', 'leaguetournaments') }}

),

renamed as (

    select

        ----------  ids
        leaguetournamentid as leaderboard_id,
        leagueid as league_id,

        ---------- strings
        title as leaderboard_name,
        tournamenttype as leaderboard_type,

        ---------- numerics

        ---------- booleans
        isvisible as is_leaderboard_visible,

        ---------- dates
        cast(createdat as date) as created_date,
        cast(
            convert_timezone('UTC', 'America/New_York', createdat) as date
        ) as created_date_et,

        ---------- timestamps
        createdat as created_at,
        startutc as leaderboard_starts_at,
        endutc as leaderboard_ends_at,
        convert_timezone('UTC', 'America/New_York', createdat) as created_at_et,
        convert_timezone('UTC', 'America/New_York', startutc) as starts_at_et,
        convert_timezone('UTC', 'America/New_York', endutc) as ends_at_et

    from source

)

select * from renamed
