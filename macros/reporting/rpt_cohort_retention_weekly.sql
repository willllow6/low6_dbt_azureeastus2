{#
    Standard rpt_{domain}__cohort_retention_weekly.
    Grain: cohort_week x activity_week x client_id x tenant_id x game_type.

    cohort_week is the Monday of the week of the user's first entry (local
    time), not registration; users who never entered are excluded.

    entries     fct_{domain}__entries (user_id, entry_date_et, client_id,
                tenant_id, tenant_name, game_type)
#}
{% macro rpt_cohort_retention_weekly(entries) %}

with

entries as (

    select *
    from {{ entries }}

),

tenant_names as (

    select
        client_id,
        tenant_id,
        game_type,
        max(coalesce(tenant_name, client_id)) as tenant_name
    from entries
    group by client_id, tenant_id, game_type

),

user_cohorts as (

    select
        user_id,
        client_id,
        tenant_id,
        game_type,
        cast(date_trunc('week', min(entry_date_et)) as date) as cohort_week
    from entries
    group by user_id, client_id, tenant_id, game_type

),

cohort_sizes as (

    select
        cohort_week,
        client_id,
        tenant_id,
        game_type,
        count(*) as cohort_size
    from user_cohorts
    group by cohort_week, client_id, tenant_id, game_type

),

user_activity_weeks as (

    select distinct
        user_id,
        client_id,
        tenant_id,
        game_type,
        cast(date_trunc('week', entry_date_et) as date) as activity_week
    from entries

),

retained as (

    select
        user_cohorts.cohort_week,
        user_activity_weeks.activity_week,
        user_cohorts.client_id,
        user_cohorts.tenant_id,
        user_cohorts.game_type,
        count(distinct user_activity_weeks.user_id) as retained_users
    from user_activity_weeks
    inner join user_cohorts
        on user_activity_weeks.user_id = user_cohorts.user_id
        and user_activity_weeks.client_id = user_cohorts.client_id
        and user_activity_weeks.tenant_id = user_cohorts.tenant_id
        and user_activity_weeks.game_type = user_cohorts.game_type
    group by 1, 2, 3, 4, 5

)

select
    retained.cohort_week,
    retained.activity_week,
    retained.client_id,
    retained.tenant_id,
    tenant_names.tenant_name,
    retained.game_type,

    datediff('week', retained.cohort_week, retained.activity_week) as weeks_since_cohort,
    cohort_sizes.cohort_size,
    retained.retained_users,
    round(retained.retained_users / cohort_sizes.cohort_size, 4) as retention_rate

from retained
inner join cohort_sizes
    on retained.cohort_week = cohort_sizes.cohort_week
    and retained.client_id = cohort_sizes.client_id
    and retained.tenant_id = cohort_sizes.tenant_id
    and retained.game_type = cohort_sizes.game_type
inner join tenant_names
    on retained.client_id = tenant_names.client_id
    and retained.tenant_id = tenant_names.tenant_id
    and retained.game_type = tenant_names.game_type

{% endmacro %}
