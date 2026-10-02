{#
    Standard rpt_{domain}__growth_metrics_daily.
    Grain: date_day x client_id x tenant_id x game_type, over a gap-free date
    spine from start_date to today (or var reporting_as_of_date when set).

    users       dim_{domain}__users (registered_at, client_id, tenant_id, game_type)
    entries     fct_{domain}__entries (entry_date_et, user_entry_number, ...)
    activity    fct_{domain}__user_activity_daily (activity_date_et, user_id, ...)
    registrations_by_tenant
                false when one registration covers the whole game: registrations
                is then null rather than repeating a game-wide count on every row.
    extra_metrics
                {name: aggregate expression over entries}, appended after the
                standard columns. Must not change the grain.
#}
{% macro rpt_growth_metrics_daily(
    users,
    entries,
    activity,
    start_date,
    registrations_by_tenant=true,
    local_timezone=var('local_timezone'),
    extra_metrics={}
) %}

{%- set as_of_date = var('reporting_as_of_date', none) -%}
{%- set end_date = "cast('" ~ as_of_date ~ "' as date)" if as_of_date else "current_date()" -%}

with

users as (

    select *
    from {{ users }}

),

entries as (

    select *
    from {{ entries }}

),

activity as (

    select *
    from {{ activity }}

),

date_spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ start_date ~ "' as date)",
        end_date="dateadd(day, 1, " ~ end_date ~ ")"
    ) }}

),

combinations as (

    select
        client_id,
        tenant_id,
        max(tenant_name) as tenant_name,
        game_type
    from entries
    group by client_id, tenant_id, game_type

),

spine as (

    select
        cast(date_spine.date_day as date) as date_day,
        combinations.client_id,
        combinations.tenant_id,
        combinations.tenant_name,
        combinations.game_type
    from date_spine
    cross join combinations

),

registrations_daily as (

    select
        cast(convert_timezone('UTC', '{{ local_timezone }}', registered_at) as date) as date_day,
        client_id,
        tenant_id,
        game_type,
        count(*) as registrations
    from users
    group by 1, 2, 3, 4

),

entries_daily as (

    select
        entry_date_et as date_day,
        client_id,
        tenant_id,
        game_type,
        count(*) as entries,
        count_if(user_entry_number = 1) as first_entries
        {%- for name, expression in extra_metrics.items() %},
        {{ expression }} as {{ name }}
        {%- endfor %}
    from entries
    group by 1, 2, 3, 4

),

active_users as (

    select
        spine.date_day,
        spine.client_id,
        spine.tenant_id,
        spine.game_type,
        count(distinct case when activity.activity_date_et = spine.date_day then activity.user_id end) as dau,
        count(distinct case when activity.activity_date_et > dateadd(day, -7, spine.date_day) then activity.user_id end) as wau,
        count(distinct activity.user_id) as mau
    from spine
    inner join activity
        on activity.client_id = spine.client_id
        and activity.tenant_id = spine.tenant_id
        and activity.game_type = spine.game_type
        and activity.activity_date_et > dateadd(day, -28, spine.date_day)
        and activity.activity_date_et <= spine.date_day
    group by 1, 2, 3, 4

)

select
    spine.date_day,
    spine.client_id,
    spine.tenant_id,
    spine.tenant_name,
    spine.game_type,

    {% if registrations_by_tenant -%}
    coalesce(registrations_daily.registrations, 0) as registrations,
    {%- else -%}
    cast(null as integer) as registrations,
    {%- endif %}
    coalesce(entries_daily.entries, 0) as entries,
    coalesce(active_users.dau, 0) as dau,
    coalesce(active_users.wau, 0) as wau,
    coalesce(active_users.mau, 0) as mau,
    coalesce(entries_daily.first_entries, 0) as first_entries
    {%- for name in extra_metrics %},
    entries_daily.{{ name }}
    {%- endfor %}

from spine
left join registrations_daily
    on spine.date_day = registrations_daily.date_day
    and spine.client_id = registrations_daily.client_id
    and spine.tenant_id = registrations_daily.tenant_id
    and spine.game_type = registrations_daily.game_type
left join entries_daily
    on spine.date_day = entries_daily.date_day
    and spine.client_id = entries_daily.client_id
    and spine.tenant_id = entries_daily.tenant_id
    and spine.game_type = entries_daily.game_type
left join active_users
    on spine.date_day = active_users.date_day
    and spine.client_id = active_users.client_id
    and spine.tenant_id = active_users.tenant_id
    and spine.game_type = active_users.game_type

{% endmacro %}
