with users as (
    select * 
    from {{ ref('stg_cfl_fantasy__users') }}
    where is_user

),

daily_registrations as (
    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', registered_at) as date) as date_day,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        registration_type,
        count(*) as new_registrations,
        0 as profile_completions,   -- no profile completion tracking in source
        0 as marketing_consents     -- no marketing consent tracking in source
    from users
    group by 1, 2, 3, 4, 5, 6
)

select * from daily_registrations
