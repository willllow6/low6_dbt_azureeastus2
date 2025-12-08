with

monthly_user_activity as (

    select
        region,
        month_start,
        sum(was_registered_this_month) as registrations,
        sum(was_active_this_month) as active_users,
        sum(prev_active) as active_users_last_month,
        sum(was_retained_this_month) as retentions,
        sum(entries_this_month) as entries
    from {{ ref('betway_picks__monthly_user_activity') }}
    group by 1,2

)

select * from monthly_user_activity