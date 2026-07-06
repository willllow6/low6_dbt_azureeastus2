with

matchups as (

    select *
    from {{ ref('stg_fanstake_rivals__user_matchups') }}

),

users as (

    select *
    from {{ ref('fanstake_rivals__users') }}

),

weekly_periods as (

    select *
    from {{ ref('stg_fanstake_rivals__weekly_periods') }}

),

joined as (

    select
        matchups.matchup_id,
        matchups.weekly_period_id,
        matchups.user1_id,
        matchups.user2_id,
        matchups.winner_id,

        users1.username as user1_username,
        users2.username as user2_username,

        matchups.status as matchup_status,

        weekly_periods.weekly_period_name as contest_name,
        weekly_periods.sport as contest_sport,
        weekly_periods.league as contest_league,
        weekly_periods.season as contest_season,
        weekly_periods.week_number as contest_week_number,
        weekly_periods.start_date_et as contest_start_date_et,
        weekly_periods.status as contest_status,

        case 
            when users2.user_role != 'user' 
                then 'AI' 
            else 'User' 
        end as matchup_type,
        
        matchups.user1_score,
        matchups.user2_score,
        
        matchups.created_date,
        matchups.created_date_et,

        matchups.created_at,
        matchups.updated_at
    from matchups
    left join users as users1
        on matchups.user1_id = users1.user_id
    left join users as users2
        on matchups.user2_id = users2.user_id
    left join weekly_periods
        on matchups.weekly_period_id = weekly_periods.weekly_period_id

)

select * from joined