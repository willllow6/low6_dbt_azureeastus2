with

weekly_leaderboards as (

    select *
    from {{ ref('betway_picks__aggregate_leaderboard_positions') }}
    where period_type = 'week'

),

last_weeks_winners as (

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
    from weekly_leaderboards
    where 
        date_trunc('week', period_end) = dateadd(week,-1,date_trunc('week',CURRENT_DATE))
        and leaderboard_rank < 3
    order by region, leaderboard_position

)

select * from last_weeks_winners
