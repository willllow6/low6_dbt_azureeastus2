-- cohort_week = Monday of the week the user made their first entry (not registration date).
-- weeks_since_cohort = 0 (first-entry week, always 100%), 1 (week after), etc.
-- Users who have never entered are excluded entirely — they do not pad cohort_size.
with entries as (
    select * 
    from {{ ref('fct_cfl_fantasy__entries') }}
    where is_auto_team = false
),

entries_with_names as (
    select
        user_id,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        entered_at
    from entries
),

cohort_weeks as (
    select
        user_id,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        date_trunc('week', min(entered_at))::date as cohort_week
    from entries_with_names
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
        cw.cohort_week,
        cw.client_id,
        cw.tenant_id,
        cw.tenant_name,
        cw.game_type,
        date_trunc('week', ewn.entered_at)::date as activity_week,
        ewn.user_id
    from entries_with_names ewn
    inner join cohort_weeks cw
        on ewn.user_id = cw.user_id
        and ewn.client_id = cw.client_id
        and ewn.tenant_id = cw.tenant_id
        and ewn.tenant_name = cw.tenant_name
        and ewn.game_type = cw.game_type
),

retention as (
    select
        uca.cohort_week,
        uca.activity_week,
        uca.client_id,
        uca.tenant_id,
        uca.tenant_name,
        uca.game_type,
        datediff('week', uca.cohort_week, uca.activity_week) as weeks_since_cohort,
        cs.cohort_size,
        count(distinct uca.user_id) as retained_users,
        count(distinct uca.user_id)::float / cs.cohort_size as retention_rate
    from user_cohort_activity uca
    inner join cohort_sizes cs
        on uca.cohort_week = cs.cohort_week
        and uca.client_id = cs.client_id
        and uca.tenant_id = cs.tenant_id
        and uca.tenant_name = cs.tenant_name
        and uca.game_type = cs.game_type
    group by 1, 2, 3, 4, 5, 6, 7, 8
)

select * from retention
