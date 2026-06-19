with

current_matches as (

    select * from {{ ref('stg_saracen_bracket__matches') }}

),

historical_matches as (

    select * from {{ ref('stg_saracen_bracket_nbafinals2026__matches') }}

),

unioned as (

    select * from current_matches
    union all
    select * from historical_matches

),

tournament_scoped as (

    select
        tournament_name,
        tournament_name || '-' || match_id::varchar as match_id,
        tournament_name || '-' || conference_id::varchar as conference_id,
        tournament_name || '-' || round_id::varchar as round_id,
        tournament_name || '-' || home_team_id::varchar as home_team_id,
        tournament_name || '-' || away_team_id::varchar as away_team_id,
        case
            when winner_team_id is null then null
            else tournament_name || '-' || winner_team_id::varchar
        end as winner_team_id,
        contest_name,
        match_slot,
        result,
        is_scored,
        created_date,
        created_at,
        match_starts_at
    from unioned

)

select * from tournament_scoped
