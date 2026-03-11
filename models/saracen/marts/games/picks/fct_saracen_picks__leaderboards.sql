with

contest_leaderboards as (

    select
        l.contest_leaderboard_id as leaderboard_id,
        l.user_id,
        l.contest_id,
        'contest' as leaderboard_type,
        -- c.region,
        null as period_type,
        null as period_start,
        null as period_end,
        c.contest_name,
        c.contest_status,
        c.contest_opens_at,
        c.contest_opens_at_et,
        c.contest_starts_at,
        c.contest_starts_at_et,
        l.points,
        l.leaderboard_position,
        l.tiebreaker_margin,
        l.created_at,
        l.updated_at
    from {{ ref('stg_saracen_picks__contest_leaderboards') }} as l
    left join {{ ref('dim_saracen_picks__contests') }} as c
        on l.contest_id = c.contest_id

),

period_leaderboards as (

    select
        pl.period_leaderboard_id as leaderboard_id,
        pl.user_id,
        null as contest_id,
        'period' as leaderboard_type,
        -- l.league_name as region,
        pl.period_type,
        pl.period_start,
        pl.period_end,
        null as contest_title,
        null as contest_status,
        null as contest_opens_at,
        null as contest_opens_at_et,
        null as contest_starts_at,
        null as contest_starts_at_et,
        pl.points,
        pl.leaderboard_position,
        null as tiebreaker_margin,
        pl.created_at,
        pl.updated_at
    from {{ ref('stg_saracen_picks__period_leaderboards') }} as pl
    left join {{ ref('dim_saracen_picks__leagues') }} as l 
        on pl.league_code = l.league_code
    
)

select *
from contest_leaderboards

union all

select *
from period_leaderboards