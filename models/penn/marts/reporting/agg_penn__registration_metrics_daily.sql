with

users as (

    select
        user_id,
        'penn' as client_id,
        'penn' as tenant_id,
        'Penn' as tenant_name,
        'squads' as game_type,
        registration_type,
        registration_date_et as registration_date
    from {{ ref('dim_penn__users') }}

),

daily as (

    select
        registration_date as date_day,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        registration_type,
        count(distinct user_id) as new_registrations,
        0 as profile_completions,   -- not captured in source schema
        0 as marketing_consents     -- not captured in source schema
    from users
    group by
        registration_date,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        registration_type

)

select * from daily
