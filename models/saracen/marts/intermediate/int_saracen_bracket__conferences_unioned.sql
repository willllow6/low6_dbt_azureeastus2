with

current_conferences as (

    select * from {{ ref('stg_saracen_bracket__conferences') }}

),

historical_conferences as (

    select * from {{ ref('stg_saracen_bracket_nbafinals2026__conferences') }}

),

unioned as (

    select * from current_conferences
    union all
    select * from historical_conferences

),

tournament_scoped as (

    select
        tournament_name,
        tournament_name || '-' || conference_id::varchar as conference_id,
        conference_name,
        competition_name,
        created_date,
        created_at
    from unioned

)

select * from tournament_scoped
