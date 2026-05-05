with

entries as (
    select * from {{ ref('fct_bet365_uf__entries') }}
),

users as (
    select user_id, is_tester from {{ ref('stg_bet365_uf__users') }}
),

tenants as (
    select tenant_id, tenant_name from {{ ref('stg_bet365_uf__tenants') }}
),

non_tester_entries as (

    select entries.*
    from entries
    left join users on entries.user_id = users.user_id
    where users.is_tester = false

),

daily_entry_metrics as (

    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', entries.entered_at) as date) as date_day,
        entries.client_id,
        entries.tenant_id,
        tenants.tenant_name,
        entries.game_type,
        count(distinct entries.contest_id) as contests_active,
        count(*) as total_entries,
        count(distinct entries.user_id) as unique_entrants,
        count(distinct case when entries.entry_number = 1 then entries.user_id end) as first_time_entrants
    from non_tester_entries as entries
    left join tenants
        on entries.tenant_id = tenants.tenant_id
    group by 1, 2, 3, 4, 5

)

select
    date_day,
    client_id,
    tenant_id,
    tenant_name,
    game_type,
    contests_active,
    total_entries,
    unique_entrants,
    first_time_entrants
from daily_entry_metrics
