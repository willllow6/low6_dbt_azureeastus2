with

aggregate_leaderboards as (

    select * 
    from {{ ref('stg_bet99_picks__aggregate_leaderboards') }}

),

users as (

    select * 
    from {{ ref('stg_bet99_picks__users') }}

),

entries as (

    select *
    from {{ ref('bet99_picks__entries') }}

),

leagues as (

    select *
    from {{ ref('stg_bet99_picks__leagues') }}

),

joined as (

    select
        aggregate_leaderboards.aggregate_leaderboard_id,
        aggregate_leaderboards.user_id,
        users.sso_user_id,

        users.username,

        leagues.league_name as region,
        aggregate_leaderboards.period_type,
        aggregate_leaderboards.period_start,
        aggregate_leaderboards.period_end,
        aggregate_leaderboards.contest_data,
        aggregate_leaderboards.total_points,
        aggregate_leaderboards.leaderboard_position,
        aggregate_leaderboards.created_at,
        aggregate_leaderboards.updated_at,

        min(entries.entered_at) as first_entered_at
    
    from aggregate_leaderboards
    left join users
        on aggregate_leaderboards.user_id = users.user_id
    left join leagues
        on aggregate_leaderboards.league_code = leagues.league_code
    left join entries
        on aggregate_leaderboards.user_id = entries.user_id
        and aggregate_leaderboards.period_start <= entries.contest_starts_at
        and aggregate_leaderboards.period_end >= entries.contest_starts_at
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13

),

new_rank as (

    select
        *,
        dense_rank() over (partition by region, period_type, period_start order by total_points desc) as leaderboard_rank
    from joined
)


select * from new_rank





