with

current_teams as (

    select * from {{ ref('stg_saracen_bracket__teams') }}

),

historical_teams as (

    select * from {{ ref('stg_saracen_bracket_nbafinals2026__teams') }}

),

unioned as (

    select * from current_teams
    union all
    select * from historical_teams

),

tournament_scoped as (

    select
        tournament_name,
        tournament_name || '-' || team_id::varchar as team_id,
        team_name,
        contest_name,
        team_rank,
        possible_points,
        win_percentage,
        created_date,
        created_at
    from unioned

)

select * from tournament_scoped
