with

entries as (

    select *
    from {{ ref('fct_opap_spintowin__entries') }}

),

contests as (

    select
        contest_id,
        tenant_id,
        tenant_name
    from {{ ref('stg_opap_spintowin__contests') }}

),

entries_with_names as (

    select
        e.entry_id,
        e.user_id,
        e.contest_id,
        e.client_id,
        coalesce(c.tenant_id,   e.tenant_id)    as tenant_id,
        coalesce(c.tenant_name, e.client_id)    as tenant_name,
        e.game_type,
        e.entry_date_et
    from entries e
    left join contests c
        on e.contest_id = c.contest_id

),

cohort_weeks as (

    select
        user_id,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        date_trunc('week', min(entry_date_et))::date as cohort_week
    from entries_with_names
    group by 1, 2, 3, 4, 5

),

user_activity_weeks as (

    select distinct
        user_id,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        date_trunc('week', entry_date_et)::date as activity_week
    from entries_with_names

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
        ua.user_id,
        ua.activity_week,
        cw.cohort_week,
        ua.client_id,
        ua.tenant_id,
        ua.tenant_name,
        ua.game_type
    from user_activity_weeks ua
    inner join cohort_weeks cw
        on  ua.user_id      = cw.user_id
        and ua.client_id    = cw.client_id
        and ua.tenant_id    = cw.tenant_id
        and ua.tenant_name  = cw.tenant_name
        and ua.game_type    = cw.game_type

),

retention as (

    select
        uca.cohort_week,
        uca.activity_week,
        uca.client_id,
        uca.tenant_id,
        uca.tenant_name,
        uca.game_type,
        datediff('week', uca.cohort_week, uca.activity_week)    as weeks_since_cohort,
        cs.cohort_size,
        count(distinct uca.user_id)                             as retained_users,
        round(count(distinct uca.user_id) / cs.cohort_size::float, 4) as retention_rate
    from user_cohort_activity uca
    inner join cohort_sizes cs
        on  uca.cohort_week     = cs.cohort_week
        and uca.client_id       = cs.client_id
        and uca.tenant_id       = cs.tenant_id
        and uca.tenant_name     = cs.tenant_name
        and uca.game_type       = cs.game_type
    group by 1, 2, 3, 4, 5, 6, 7, 8

)

select * from retention
