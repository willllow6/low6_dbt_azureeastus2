select
    l.leaderboard_id,
    l.user_id,
    l.contest_id,
    u.sso_user_id,
    u.username,
    u.email,
    l.leaderboard_type,
    -- l.region,
    l.period_type,
    l.period_start,
    l.period_end,
    l.contest_name,
    l.contest_status,
    l.contest_opens_at,
    l.contest_opens_at_et,
    l.contest_starts_at,
    l.contest_starts_at_et,
    l.points,
    l.leaderboard_position,
    l.tiebreaker_margin,
    l.created_at,
    l.updated_at
from {{ ref('fct_saracen_picks__leaderboards') }} as l 
left join {{ ref('dim_saracen_picks__users') }} as u 
    on l.user_id = u.user_id


