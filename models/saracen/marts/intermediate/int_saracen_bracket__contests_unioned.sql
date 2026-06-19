with

current_contests as (

    select * from {{ ref('stg_saracen_bracket__contests') }}

),

historical_contests as (

    select * from {{ ref('stg_saracen_bracket_nbafinals2026__contests') }}

),

unioned as (

    select * from current_contests
    union all
    select * from historical_contests

),

tournament_scoped as (

    select
        tournament_name,
        tournament_name || '-' || contest_id::varchar as contest_id,
        'BKT_' || tournament_name || '-' || contest_name as contest_sk,
        contest_name,
        contest_status,
        tie_breaker_answer,
        allow_multiple_brackets,
        contest_opens_at,
        contest_opens_at_et,
        contest_starts_at,
        contest_starts_at_et,
        created_at
    from unioned

)

select * from tournament_scoped
