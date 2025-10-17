with

aggregate_leaderboards as (

    select * 
    from {{ ref('stg_betway_picks__aggregate_leaderboards') }}

),

users as (

    select * 
    from {{ ref('stg_betway_picks__users') }}

),

leagues as (

    select *
    from {{ ref('stg_betway_picks__leagues') }}

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
        aggregate_leaderboards.updated_at
    
    from aggregate_leaderboards
    left join users
        on aggregate_leaderboards.user_id = users.user_id
    left join leagues
        on aggregate_leaderboards.league_code = leagues.league_code

)

select * from joined





