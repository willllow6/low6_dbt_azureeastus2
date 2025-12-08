with

weekly_user_activity as (

    select
        region,
        week_start,
        sum(was_registered_this_week) as registrations,
        sum(was_active_this_week) as active_users,
        sum(prev_active) as active_users_last_week,
        sum(was_retained_this_week) as retentions,
        sum(entries_this_week) as entries
    from {{ ref('betway_picks__weekly_user_activity') }}
    group by 1,2

)

select * from weekly_user_activity