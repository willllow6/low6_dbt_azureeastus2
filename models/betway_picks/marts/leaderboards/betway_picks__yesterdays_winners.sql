with

contest_leaderboards as (

    select *
    from {{ ref('betway_picks__contest_leaderboard_positions') }}

),

yesterdays_winners as (

    select
        region,
        contest_title,
        contest_start_date,
        leaderboard_position,
        username,
        betway_id,
        points,
        tiebreaker_prediction,
        tiebreaker_outcome,
        tiebreaker_margin,
        entered_at,
        entered_at_et
    from contest_leaderboards
    where 
        contest_start_date = CURRENT_DATE() - 1
        and leaderboard_position <= 10
    order by region, leaderboard_position

)

select * from yesterdays_winners
