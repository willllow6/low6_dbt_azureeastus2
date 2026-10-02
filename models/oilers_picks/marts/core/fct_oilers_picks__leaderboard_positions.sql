with

entries as (

    select *
    from {{ ref('fct_oilers_picks__entries') }}

),

contests as (

    select *
    from {{ ref('dim_oilers_picks__contests') }}

),

period_leaderboards as (

    select *
    from {{ ref('stg_oilers_picks__period_leaderboards') }}

),

completed_entries as (

    select
        entries.user_id,
        entries.contest_id,
        entries.entry_points,
        entries.tiebreak_error,
        contests.contest_name,
        contests.starts_at as contest_starts_at
    from entries
    inner join contests
        on entries.contest_id = contests.contest_id
    where contests.contest_status = 'COMPLETE'

),

-- Month and season leaderboards on the game-wide 'Overall League' (id 1).
-- Friend-league period leaderboards are not ranked.
overall_period_leaderboards as (

    select *
    from period_leaderboards
    where friend_league_id = '1'

),

period_totals as (

    select
        overall_period_leaderboards.period_leaderboard_id,
        overall_period_leaderboards.friend_league_id,
        overall_period_leaderboards.client_id,
        overall_period_leaderboards.tenant_id,
        overall_period_leaderboards.tenant_name,
        overall_period_leaderboards.game_type,
        overall_period_leaderboards.period_leaderboard_name,
        overall_period_leaderboards.period_type,
        overall_period_leaderboards.period_start,
        overall_period_leaderboards.period_end,
        completed_entries.user_id,
        count(*) as entries,
        sum(completed_entries.entry_points) as total_points
    from overall_period_leaderboards
    inner join completed_entries
        on completed_entries.contest_starts_at >= overall_period_leaderboards.period_start
        and completed_entries.contest_starts_at <= overall_period_leaderboards.period_end
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11

),

unioned as (

    select
        'CONTEST' as leaderboard_type,
        cast(null as varchar) as period_leaderboard_id,
        contest_id,
        cast(null as varchar) as friend_league_id,
        'oilers' as client_id,
        'oilers' as tenant_id,
        'Edmonton Oilers' as tenant_name,
        'pickem' as game_type,
        contest_name as leaderboard_name,
        contest_starts_at as leaderboard_starts_at,
        contest_starts_at as leaderboard_ends_at,
        user_id,
        1 as entries,
        entry_points as total_points,
        tiebreak_error
    from completed_entries

    union all

    select
        period_type as leaderboard_type,
        period_leaderboard_id,
        cast(null as varchar) as contest_id,
        friend_league_id,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        period_leaderboard_name as leaderboard_name,
        period_start as leaderboard_starts_at,
        period_end as leaderboard_ends_at,
        user_id,
        entries,
        total_points,
        cast(null as number) as tiebreak_error
    from period_totals

)

select
    leaderboard_type,
    period_leaderboard_id,
    contest_id,
    friend_league_id,
    user_id,

    client_id,
    tenant_id,
    tenant_name,
    game_type,

    leaderboard_name,

    entries,
    total_points,
    tiebreak_error,
    -- Ranked within each leaderboard, not by name: month names repeat across
    -- seasons (e.g. 'November' 2024 and 2025).
    rank() over (
        partition by leaderboard_type, period_leaderboard_id, contest_id
        order by total_points desc nulls last, tiebreak_error asc
    ) as leaderboard_position,

    leaderboard_starts_at,
    leaderboard_ends_at

from unioned
