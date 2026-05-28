with leagues as (
    select * from {{ ref('stg_cfl_fantasy__leagues') }}
),

team_counts as (
    select
        contest_id,
        count(*) as current_team_count,
        count_if(is_auto_team = false) as user_team_count,
        count_if(is_auto_team) as bot_team_count 
    from {{ ref('stg_cfl_fantasy__teams') }}
    group by 1
),

draft_info as (
    select
        contest_id,
        draft_id,
        draft_type,
        draft_status,
        started_at as draft_started_at,
        ended_at as draft_ended_at
    from {{ ref('stg_cfl_fantasy__drafts') }}
),

enriched as (
    select
        l.contest_id,
        l.league_code,
        l.client_id,
        l.tenant_id,
        l.tenant_name,
        l.game_type,
        l.contest_name,
        l.contest_status,
        l.is_active,
        l.max_teams,
        l.playoff_teams,
        l.trade_deadline_week,
        l.is_private,
        l.is_auto_fill,
        l.is_setup_complete,
        l.setup_step,
        l.draft_type,
        l.trade_approval,
        l.selection_time_seconds,
        l.draft_day,
        l.draft_starts_at,
        l.created_at,
        coalesce(tc.current_team_count, 0) as current_team_count,
        coalesce(tc.user_team_count, 0) as user_team_count,
        coalesce(tc.bot_team_count, 0) as bot_team_count,
        di.draft_id,
        coalesce(di.draft_status, 'upcoming') as draft_status,
        di.draft_started_at,
        di.draft_ended_at

    from leagues l
    left join team_counts tc on l.contest_id = tc.contest_id
    left join draft_info di on l.contest_id = di.contest_id
)

select * 
from enriched
where draft_status not in ('live','paused')
