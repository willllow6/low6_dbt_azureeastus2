with

users as (
    select *
    from {{ ref('stg_bet365_uf__users') }}
    where is_tester = false
),

daily_registrations as (

    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', registered_at) as date) as date_day,
        client_id,
        -- tenant_id,
        -- 'Bet365' as tenant_name,
        game_type,
        registration_type,
        count(*) as new_registrations,
        sum(case when has_completed_profile = true then 1 else 0 end) as profile_completions,
        sum(case when has_consented_marketing = true then 1 else 0 end) as marketing_consents
    from users
    group by 1, 2, 3, 4

)

select
    date_day,
    client_id,
    -- tenant_id,
    -- tenant_name,
    game_type,
    registration_type,
    new_registrations,
    profile_completions,
    marketing_consents
from daily_registrations
