with

monthly_leaderboards as (

    select *
    from {{ ref('betway_picks__aggregate_leaderboard_positions') }}
    where period_type = 'month'

),

last_months_winners as (

    select
        betway_SubscriberKey as SubscriberKey,
        betway_UserId as UserId,
        betway_CasinoId as CasinoId,
        leaderboard_competition,
        'EN' as language,
        case
            when leaderboard_rank = 1
                then 'First'
            when leaderboard_rank = 2
                then 'Second'
        end as first_or_second
    from monthly_leaderboards
    where 
        date_trunc('month', period_end) = dateadd(month,-1,date_trunc('month',sysdate()))
        and leaderboard_rank < 2
    order by region, leaderboard_competition, leaderboard_position

)

select * from last_months_winners
