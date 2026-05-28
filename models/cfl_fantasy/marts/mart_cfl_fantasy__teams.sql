with entries as (
    select * from {{ ref('fct_cfl_fantasy__entries') }}
),

contests as (
    select
        contest_id,
        contest_name,
        contest_status,
        max_teams,
        is_private,
        draft_type,
        draft_status
    from {{ ref('dim_cfl_fantasy__contests') }}
),

users as (
    select
        user_id,
        display_name as user_display_name
    from {{ ref('dim_cfl_fantasy__users') }}
),

score_totals as (
    select
        entry_id,
        sum(points) as total_points,
        count(distinct fantasy_week) as weeks_played
    from {{ ref('stg_cfl_fantasy__scores') }}
    group by 1
),

enriched as (
    select
        e.entry_id,
        e.contest_id,
        e.user_id,
        e.client_id,
        e.tenant_id,
        e.tenant_name,
        e.game_type,
        e.team_name,
        e.is_commissioner,
        e.is_auto_draft,
        e.is_auto_team,
        e.waiver_priority,
        e.entry_number,
        e.entered_at,
        u.user_display_name,
        c.contest_name as league_name,
        c.contest_status as league_status,
        c.max_teams,
        c.is_private as is_private_league,
        c.draft_type,
        c.draft_status,
        coalesce(st.total_points, 0) as total_points,
        coalesce(st.weeks_played, 0) as weeks_played

    from entries e
    left join contests c on e.contest_id = c.contest_id
    left join users u on e.user_id = u.user_id
    left join score_totals st on e.entry_id = st.entry_id
)

select * from enriched
