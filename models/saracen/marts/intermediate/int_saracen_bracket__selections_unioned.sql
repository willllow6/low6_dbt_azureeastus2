with

current_selections as (

    select * from {{ ref('stg_saracen_bracket__selections') }}

),

historical_selections as (

    select * from {{ ref('stg_saracen_bracket_nbafinals2026__selections') }}

),

unioned as (

    select * from current_selections
    union all
    select * from historical_selections

),

tournament_scoped as (

    select
        tournament_name,
        tournament_name || '-' || selection_id::varchar as selection_id,
        tournament_name || '-' || entry_id::varchar as entry_id,
        tournament_name || '-' || match_id::varchar as match_id,
        tournament_name || '-' || selected_team_id::varchar as selected_team_id,
        created_date,
        created_at,
        updated_at
    from unioned

)

select * from tournament_scoped
