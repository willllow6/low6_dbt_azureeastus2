with

contests as (
    select * from {{ ref('dim_bet365_uf__contests') }}
),

tenants as (
    select tenant_id, tenant_name from {{ ref('stg_bet365_uf__tenants') }}
),

entries as (
    select contest_id, count(*) as entry_count
    from {{ ref('fct_bet365_uf__entries') }}
    group by 1
),

daily_contest_metrics as (

    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', contests.starts_at) as date) as date_day,
        contests.client_id,
        contests.tenant_id,
        tenants.tenant_name,
        contests.game_type,
        sum(case when contests.contest_status = 'scheduled' then 1 else 0 end) as contests_scheduled,
        sum(case when contests.contest_status = 'live' then 1 else 0 end) as contests_live,
        sum(case when contests.contest_status = 'completed' then 1 else 0 end) as contests_completed,
        div0(
            sum(coalesce(entries.entry_count, 0)),
            count(distinct case when coalesce(entries.entry_count, 0) > 0 then contests.contest_id end)
        ) as avg_entries_per_contest
    from contests
    left join tenants
        on contests.tenant_id = tenants.tenant_id
    left join entries
        on contests.contest_id = entries.contest_id
    group by 1, 2, 3, 4, 5

)

select
    date_day,
    client_id,
    tenant_id,
    tenant_name,
    game_type,
    contests_scheduled,
    contests_live,
    contests_completed,
    avg_entries_per_contest
from daily_contest_metrics
