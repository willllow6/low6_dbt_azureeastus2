with

source as (

    select *
    from {{ source('saracen_bracket_nbafinals2026', 'selectionmatches') }}

),

renamed as (

    select

        ----------  ids
        selectionmatchesid as selection_id,
        userselectionid as entry_id,
        matchid as match_id,
        winnerteamid as selected_team_id,

        ---------- strings
        'nba_finals_2026' as tournament_name,

        ---------- numerics

        ---------- booleans

        ---------- dates
        cast(createdat as date) as created_date,

        ---------- timestamps
        createdat as created_at,
        updatedat as updated_at

    from source

)

select * from renamed
