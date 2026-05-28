with entries as (
    select * 
    from {{ ref('fct_cfl_fantasy__entries') }}
    where is_auto_team = false
),

daily_metrics as (
    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', entered_at) as date) as date_day,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        count(distinct contest_id) as contests_active,
        count(*) as total_entries,
        count(distinct user_id) as unique_entrants,
        count(distinct case when entry_number = 1 then user_id end) as first_time_entrants
    from entries
    group by 1, 2, 3, 4, 5
)

select * from daily_metrics
