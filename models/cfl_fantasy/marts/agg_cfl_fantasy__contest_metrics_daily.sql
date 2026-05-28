with leagues as (
    select * from {{ ref('stg_cfl_fantasy__leagues') }}
),

drafts as (
    select
        contest_id,
        draft_status
    from {{ ref('stg_cfl_fantasy__drafts') }}
),

team_counts as (
    select
        contest_id,
        count(*) as team_count,
        count_if(is_auto_team = false) as user_team_count,
        count_if(is_auto_team) as bot_team_count
    from {{ ref('stg_cfl_fantasy__teams') }}
    group by 1
),

leagues_with_status as (
    select
        l.contest_id,
        l.client_id,
        l.tenant_id,
        l.tenant_name,
        l.game_type,
        l.created_at,
        l.is_setup_complete,
        coalesce(tc.team_count, 0) as team_count,
        coalesce(tc.user_team_count, 0) as user_team_count,
        coalesce(tc.bot_team_count, 0) as bot_team_count,
        d.draft_status,
        case
            when l.is_setup_complete = false then 'scheduled'
            when d.draft_status = 'completed' then 'completed'
            else 'live'
        end as derived_status
    from leagues l
    left join drafts d on l.contest_id = d.contest_id
    left join team_counts tc on l.contest_id = tc.contest_id
),

daily_metrics as (
    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', created_at) as date) as date_day,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        sum(case when derived_status = 'scheduled' then 1 else 0 end) as contests_scheduled,
        sum(case when derived_status = 'live' then 1 else 0 end) as contests_live,
        sum(case when derived_status = 'completed' then 1 else 0 end) as contests_completed,
        avg(team_count) as avg_entries_per_contest
    from leagues_with_status
    group by 1, 2, 3, 4, 5
)

select * from daily_metrics
