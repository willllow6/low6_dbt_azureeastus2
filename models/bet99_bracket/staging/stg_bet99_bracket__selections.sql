with

source as (

    select *
    from {{ source('bet99_bracket', 'selectionmatches') }}

),

renamed as (

    select

        ----------  ids
        selectionmatchesid as selection_id,
        userselectionid as entry_id,
        matchid as match_id,
        winnerteamid as selected_team_id,

        ---------- strings
        '{{ var("bet99_bracket_current_tournament") }}' as tournament_name,

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
