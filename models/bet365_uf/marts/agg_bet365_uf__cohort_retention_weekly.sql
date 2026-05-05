with

events as (
    select * from {{ ref('fct_bet365_uf__events') }}
),

non_tester_users as (
    select user_id, client_id, game_type
    from {{ ref('stg_bet365_uf__users') }}
    where not is_tester
),

tenants as (
    select tenant_id, tenant_name from {{ ref('stg_bet365_uf__tenants') }}
),

user_tenants as (
    select distinct user_id, tenant_id
    from {{ ref('fct_bet365_uf__entries') }}
),

events_with_dims as (

    select
        events.user_id,
        non_tester_users.client_id,
        user_tenants.tenant_id,
        coalesce(tenants.tenant_name, non_tester_users.client_id) as tenant_name_resolved,
        non_tester_users.game_type,
        events.created_at
    from events
    inner join non_tester_users on events.user_id = non_tester_users.user_id
    inner join user_tenants on events.user_id = user_tenants.user_id
    left join tenants on user_tenants.tenant_id = tenants.tenant_id

),

cohort_weeks as (

    select
        user_id,
        client_id,
        tenant_id,
        tenant_name_resolved as tenant_name,
        game_type,
        min(date_trunc('week', convert_timezone('UTC', '{{ var("local_timezone") }}', created_at))) as cohort_week
    from events_with_dims
    group by 1, 2, 3, 4, 5

),

cohort_sizes as (

    select
        cohort_week,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        count(distinct user_id) as cohort_size
    from cohort_weeks
    group by 1, 2, 3, 4, 5

),

user_cohort_activity as (

    select
        cohort_weeks.user_id,
        cohort_weeks.cohort_week,
        cohort_weeks.client_id,
        cohort_weeks.tenant_id,
        cohort_weeks.tenant_name,
        cohort_weeks.game_type,
        date_trunc('week', convert_timezone('UTC', '{{ var("local_timezone") }}', events_with_dims.created_at)) as activity_week
    from cohort_weeks
    inner join events_with_dims
        on cohort_weeks.user_id = events_with_dims.user_id
        and cohort_weeks.client_id = events_with_dims.client_id
        and cohort_weeks.tenant_id = events_with_dims.tenant_id
        and cohort_weeks.tenant_name = events_with_dims.tenant_name_resolved
        and cohort_weeks.game_type = events_with_dims.game_type
    group by 1, 2, 3, 4, 5, 6, 7

),

weekly_activity as (

    select
        cohort_week,
        activity_week,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        count(distinct user_id) as retained_users
    from user_cohort_activity
    group by 1, 2, 3, 4, 5, 6

),

retention as (

    select
        weekly_activity.cohort_week,
        weekly_activity.activity_week,
        weekly_activity.client_id,
        weekly_activity.tenant_id,
        weekly_activity.tenant_name,
        weekly_activity.game_type,
        datediff('week', weekly_activity.cohort_week, weekly_activity.activity_week) as weeks_since_cohort,
        cohort_sizes.cohort_size,
        weekly_activity.retained_users,
        div0(weekly_activity.retained_users, cohort_sizes.cohort_size) as retention_rate
    from weekly_activity
    left join cohort_sizes
        on weekly_activity.cohort_week = cohort_sizes.cohort_week
        and weekly_activity.client_id = cohort_sizes.client_id
        and weekly_activity.tenant_id = cohort_sizes.tenant_id
        and weekly_activity.tenant_name = cohort_sizes.tenant_name
        and weekly_activity.game_type = cohort_sizes.game_type

)

select
    cohort_week,
    activity_week,
    client_id,
    tenant_id,
    tenant_name,
    game_type,
    weeks_since_cohort,
    cohort_size,
    retained_users,
    retention_rate
from retention
