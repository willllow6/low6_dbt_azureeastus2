-- Each row is an independent cohort snapshot for a given registration date.
-- activated = users who registered on date_day AND have ever made ≥1 entry.
-- repeat_entrants = users who registered on date_day AND have ever made ≥2 entries.
with users as (
    select * 
    from {{ ref('stg_cfl_fantasy__users') }}
    where is_user
),

entries as (
    select * 
    from {{ ref('fct_cfl_fantasy__entries') }}
    where is_auto_team = false
),

user_entry_counts as (
    select
        user_id,
        count(*) as total_entries
    from entries
    group by 1
),

cohort_data as (
    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', u.registered_at) as date) as date_day,
        u.client_id,
        u.tenant_id,
        u.tenant_name,
        u.game_type,
        u.user_id,
        coalesce(uec.total_entries, 0) as total_entries
    from users u
    left join user_entry_counts uec on u.user_id = uec.user_id
),

funnel as (
    select
        date_day,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        count(*) as registered,
        count(case when total_entries >= 1 then user_id end) as activated,
        count(case when total_entries >= 2 then user_id end) as repeat_entrants
    from cohort_data
    group by 1, 2, 3, 4, 5
)

select
    date_day,
    client_id,
    tenant_id,
    tenant_name,
    game_type,
    registered,
    activated,
    activated::float / nullif(registered, 0) as activation_rate,
    repeat_entrants,
    repeat_entrants::float / nullif(activated, 0) as repeat_rate
from funnel
