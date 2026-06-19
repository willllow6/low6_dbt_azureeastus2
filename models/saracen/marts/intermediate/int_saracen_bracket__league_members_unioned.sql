with

current_members as (

    select * from {{ ref('stg_saracen_bracket__league_members') }}

),

historical_members as (

    select * from {{ ref('stg_saracen_bracket_nbafinals2026__league_members') }}

),

unioned as (

    select * from current_members
    union all
    select * from historical_members

),

tournament_scoped as (

    select
        tournament_name,
        tournament_name || '-' || league_member_id::varchar as league_member_id,
        tournament_name || '-' || league_id::varchar as league_id,
        tournament_name || '-' || user_id::varchar as user_id,
        league_joined_date,
        league_joined_at
    from unioned

)

select * from tournament_scoped
