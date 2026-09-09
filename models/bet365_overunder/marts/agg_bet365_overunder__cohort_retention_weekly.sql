with

entries as (

    select *
    from {{ ref('fct_bet365_overunder__entries') }}

),

user_entry_weeks as (

    select
        user_id,
        client_id,
        tenant_id,
        country,
        date_trunc('week', entry_date_et) as activity_week
    from entries
    group by 1,2,3,4,5

),

ranked_user_entry_weeks as (

    select
        *,
        row_number() over (partition by user_id order by activity_week) as user_entry_week_number
    from user_entry_weeks

),

first_user_entry_weeks as (

    select
        user_id,
        activity_week as cohort_week
    from ranked_user_entry_weeks
    where user_entry_week_number = 1

),

add_cohort_weeks as (

    select
        a.user_id,
        a.client_id,
        a.tenant_id,
        a.country,
        a.activity_week,
        b.cohort_week
    from ranked_user_entry_weeks as a
    left join first_user_entry_weeks as b
        on a.user_id = b.user_id

),

cohort_sizes as (

    select
        cohort_week,
        client_id,
        tenant_id,
        country,
        count(user_id) as cohort_size
    from add_cohort_weeks
    where activity_week = cohort_week
    group by 1,2,3,4

),

retention as (

    select
        add_cohort_weeks.cohort_week,
        add_cohort_weeks.activity_week,
        add_cohort_weeks.client_id,
        add_cohort_weeks.tenant_id,
        add_cohort_weeks.country,
        datediff('week', add_cohort_weeks.cohort_week, add_cohort_weeks.activity_week) as weeks_since_cohort,
        cohort_sizes.cohort_size,
        count(add_cohort_weeks.user_id) as retained_users,
        round(count(add_cohort_weeks.user_id) / cohort_sizes.cohort_size::float, 4) as retention_rate
    from add_cohort_weeks
    inner join cohort_sizes
        on add_cohort_weeks.cohort_week = cohort_sizes.cohort_week
        and add_cohort_weeks.client_id = cohort_sizes.client_id
        and add_cohort_weeks.tenant_id = cohort_sizes.tenant_id
        and add_cohort_weeks.country = cohort_sizes.country
    group by 1,2,3,4,5,6,7

)

select * from retention
