with

current_rounds as (

    select * from {{ ref('stg_saracen_bracket__rounds') }}

),

historical_rounds as (

    select * from {{ ref('stg_saracen_bracket_nbafinals2026__rounds') }}

),

unioned as (

    select * from current_rounds
    union all
    select * from historical_rounds

),

tournament_scoped as (

    select
        tournament_name,
        tournament_name || '-' || round_id::varchar as round_id,
        round_name,
        competition_name,
        round_points,
        created_date,
        created_at
    from unioned

)

select * from tournament_scoped
