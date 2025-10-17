with

contest_leaderboards as (

    select *
    from {{ ref('betway_picks__contest_leaderboard_positions') }}

),

yesterdays_winners as (

    select
        betway_SubscriberKey as SubscriberKey,
        betway_UserId as UserId,
        betway_CasinoId as CasinoId,
        'EN' as language,
        case
            when leaderboard_rank = 1
                then 'First'
            when leaderboard_rank = 2
                then 'Second'
        end as first_or_second
    from contest_leaderboards
    where 
        contest_start_date = CURRENT_DATE() - 1
        and leaderboard_rank < 3
    order by region, leaderboard_position

)

select * from yesterdays_winners
