with

lineups as (

    select *
    from {{ ref('fanstake_rivals__lineups') }}

),

lineup_stats as (

    select
        team_name,
        lineup_selections,
        is_ai_generated,
        entry_date_et,
        entry_hour_et,
        case
            when user_entry_number = 1
                then 'First Lineup'
            else 'Returning Lineup'
        end as user_lineup_type,
        contest_name,
        contest_sport,
        contest_season,
        contest_week_number,
        contest_status,
        contest_league,
        contest_start_date_et,
        count(*) as lineups

    from lineups
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13


)

select * from lineup_stats