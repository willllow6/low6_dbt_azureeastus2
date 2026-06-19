with

current_users as (

    select * from {{ ref('stg_saracen_bracket__users') }}

),

historical_users as (

    select * from {{ ref('stg_saracen_bracket_nbafinals2026__users') }}

),

unioned as (

    select * from current_users
    union all
    select * from historical_users

),

tournament_scoped as (

    select
        tournament_name,
        tournament_name || '-' || user_id::varchar as user_id,
        sso_user_id,
        account_id,
        first_name,
        last_name,
        email,
        username,
        country_code,
        user_age,
        user_age_band,
        user_generation,
        date_of_birth,
        created_date,
        created_date_et,
        created_at,
        created_at_et
    from unioned

)

select * from tournament_scoped
