with

current_entries as (

    select * from {{ ref('stg_saracen_bracket__entries') }}

),

historical_entries as (

    select * from {{ ref('stg_saracen_bracket_nbafinals2026__entries') }}

),

unioned as (

    select * from current_entries
    union all
    select * from historical_entries

),

tournament_scoped as (

    select
        tournament_name,
        tournament_name || '-' || entry_id::varchar as entry_id,
        'BKT_' || tournament_name || '-' || entry_id::varchar as entry_sk,
        tournament_name || '-' || user_id::varchar as user_id,
        sso_user_id,
        'BKT_' || tournament_name || '-' || competition_name as contest_sk,
        game_type,
        user_bracket_name,
        competition_name,
        points,
        first_round_points,
        second_round_points,
        sweet_16_points,
        elite_eight_points,
        final_four_points,
        championship_points,
        tie_breaker_selection,
        created_date,
        created_date_et,
        created_at,
        created_at_et
    from unioned

)

select * from tournament_scoped
