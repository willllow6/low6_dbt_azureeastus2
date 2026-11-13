{%- set as_of_date = var('reporting_as_of_date', none) -%}
{%- set as_of = "cast('" ~ as_of_date ~ "' as timestamp_ntz)" if as_of_date
    else "convert_timezone('UTC', current_timestamp())::timestamp_ntz" -%}

with

contests as (

    select *
    from {{ ref('dim_bet365_pickem__contests') }}

),

funnel as (

    select *
    from {{ ref('fct_bet365_pickem__contest_funnel') }}

),

funnel_counts as (

    select
        contest_id,
        tenant_id,
        max(tenant_name) as tenant_name,
        max(country_code) as country_code,
        max(state_code) as state_code,
        count(*) as eligible_users,
        count_if(funnel_stage = 'launched_not_started') as launched_not_started,
        count_if(has_started) as started,
        count_if(funnel_stage = 'saved_not_submitted') as saved_not_submitted,
        count_if(funnel_stage = 'refused') as refused,
        count_if(has_submitted) as submitted
    from funnel
    group by contest_id, tenant_id

)

select
    funnel_counts.contest_id,
    contests.client_id,
    funnel_counts.tenant_id,
    funnel_counts.tenant_name,
    contests.game_type,

    funnel_counts.country_code,
    funnel_counts.state_code,
    contests.contest_name,
    contests.contest_status,
    contests.starts_at,
    contests.locks_at,

    funnel_counts.eligible_users,
    funnel_counts.launched_not_started,
    funnel_counts.started,
    funnel_counts.saved_not_submitted,
    funnel_counts.refused,
    funnel_counts.submitted,
    round(1 - funnel_counts.submitted / nullif(funnel_counts.eligible_users, 0), 4) as abandonment_rate,
    round(funnel_counts.saved_not_submitted / nullif(funnel_counts.started, 0), 4) as saved_abandonment_rate,

    -- rates only become final once entries close at the first fixture lock
    coalesce(contests.locks_at <= {{ as_of }}, false) as is_final

from funnel_counts
inner join contests
    on funnel_counts.contest_id = contests.contest_id
