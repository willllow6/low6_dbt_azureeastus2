with

launches as (

    select *
    from {{ ref('stg_bet365_pickem__launches') }}

),

users as (

    select *
    from {{ ref('stg_bet365_pickem__users') }}

),

-- Captured launches start on 2026-11-13, when ADF began appending them. The
-- players table adds every user's first launch (complete history) and latest
-- launch (in case it landed after the last capture).
launch_rows as (

    select
        user_id,
        launched_at,
        'captured_launch' as launch_source,
        2 as source_priority
    from launches

    union all

    select
        user_id,
        registered_at as launched_at,
        'first_launch' as launch_source,
        1 as source_priority
    from users

    union all

    select
        user_id,
        last_launched_at as launched_at,
        'last_launch' as launch_source,
        3 as source_priority
    from users

)

select
    {{ dbt_utils.generate_surrogate_key(['user_id', 'launched_at']) }} as launch_id,
    user_id,
    launched_at,
    launch_source

from launch_rows
where launched_at is not null
qualify row_number() over (
    partition by user_id, launched_at
    order by source_priority
) = 1
