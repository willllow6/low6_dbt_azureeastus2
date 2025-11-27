with

contest_leaderboards as (

    select * 
    from {{ ref('stg_bet99_picks__contest_leaderboards') }}

),

entries as (

    select *
    from  {{ ref('bet99_picks__entries') }}

),

joined as (

    select
        contest_leaderboards.contest_leaderboard_id,
        contest_leaderboards.user_id,
        entries.sso_user_id,
        contest_leaderboards.contest_id,

        entries.username,
        entries.region,
        entries.contest_title,
        entries.contest_status,
        entries.contest_start_date,
        entries.contest_start_date_et,
        entries.entered_at,
        entries.entered_at_et,
        entries.tiebreaker_prediction,
        entries.tiebreaker_outcome,
        contest_leaderboards.tiebreaker_margin,

        contest_leaderboards.points,
        contest_leaderboards.leaderboard_position,
        contest_leaderboards.created_at,
        contest_leaderboards.updated_at
    
    from contest_leaderboards
    left join entries
        on contest_leaderboards.user_id = entries.user_id
        and contest_leaderboards.contest_id = entries.contest_id

),

new_rank as (

    select
        *,
        dense_rank() over (partition by contest_id order by points desc, tiebreaker_margin) as leaderboard_rank
    from joined
)


select * from new_rank





