with

entries as (

    select *
    from {{ ref('fct_saracen_bracket__entries') }}

),

users as (

    select *
    from {{ ref('dim_saracen_bracket__users') }}

),

joined as (

    select
        entries.entry_id,
        entries.sso_user_id,
        entries.user_id,

        entries.user_bracket_name,
        entries.competition_name,
        entries.points,
        entries.first_round_points,
        entries.second_round_points,
        entries.sweet_16_points,
        entries.elite_eight_points,
        entries.final_four_points,
        entries.championship_points,
        entries.tie_breaker_selection,
        entries.created_date,
        entries.created_date_et,
        entries.created_at,
        entries.created_at_et,

        users.first_name,
        users.last_name,
        users.email,
        users.username,
        users.user_age
    from entries
    left join users 
        on entries.user_id = users.user_id

)

select * from joined