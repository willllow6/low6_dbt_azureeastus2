with

lineups as (

    select *
    from {{ ref('stg_fanstake_rivals__user_weekly_lineups') }}

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
        lineups.lineup_id,
        lineups.user_id,
        lineups.weekly_period_id,

        users.fanstake_id,
        users.username,
        users.team_name,
        users.team_sport,
        users.team_league,

        lineups.lineup_name,
        lineups.total_salary,
        lineups.remaining_salary,
        lineups.total_points,
        lineups.status,
        lineups.rank,
        lineups.lineup_selections,
        lineups.is_ai_generated,
        lineups.generated_for_user_id,
        lineups.created_date as entry_date,
        lineups.created_date_et as entry_date_et,
        lineups.created_hour as entry_hour,
        lineups.created_hour_et as entry_hour_et,
        lineups.created_at,
        lineups.updated_at,
    
        weekly_periods.weekly_period_name as contest_name,
        weekly_periods.sport as contest_sport,
        weekly_periods.season as contest_season,
        weekly_periods.week_number as contest_week_number,
        weekly_periods.status as contest_status,
        weekly_periods.league as contest_league,
        weekly_periods.start_date_et as contest_start_date_et

    from lineups
    left join users
        on lineups.user_id = users.user_id
    left join weekly_periods
        on lineups.weekly_period_id = weekly_periods.weekly_period_id
        
),

user_entry_number as (

    select 
        *,
        row_number() over (partition by user_id order by created_at) as user_entry_number
    from joined

)

select * from user_entry_number