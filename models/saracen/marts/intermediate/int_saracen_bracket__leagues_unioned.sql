with

current_leagues as (

    select * from {{ ref('stg_saracen_bracket__leagues') }}

),

historical_leagues as (

    select * from {{ ref('stg_saracen_bracket_nbafinals2026__leagues') }}

),

unioned as (

    select * from current_leagues
    union all
    select * from historical_leagues

),

tournament_scoped as (

    select
        tournament_name,
        tournament_name || '-' || league_id::varchar as league_id,
        league_owner_user_id,
        league_name,
        league_type,
        league_code,
        league_created_date,
        league_created_at
    from unioned

)

select * from tournament_scoped
