with

lineups as (

    select *
    from {{ ref('stg_fanstake_rivals__user_weekly_lineups') }}
    where is_ai_generated = false

),

athletes as (

    select *
    from {{ ref('stg_fanstake_rivals__athletes') }}
    
),

lineup_players as (

    select *
    from {{ ref('stg_fanstake_rivals__lineup_players') }}
    
),

weekly_periods as (

    select *
    from {{ ref('stg_fanstake_rivals__weekly_periods') }}
    
),

joined as (

    select
        lineup_players.lineup_player_id,
        lineup_players.lineup_id,
        lineup_players.athlete_id,
        weekly_periods.weekly_period_name as contest_name,
        weekly_periods.start_date_et as contest_start_date_et,
        weekly_periods.status as contest_status,
        weekly_periods.league as contest_league,
        weekly_periods.sport as contest_sport,
        athletes.athlete_name,
        lineup_players.position as athlete_position,
        lineup_players.slot,
        lineup_players.is_portal_pick,
        lineup_players.points,
        lineup_players.created_at,
        lineup_players.updated_at
    from lineup_players
    inner join lineups
        on lineup_players.lineup_id = lineups.lineup_id
    inner join athletes
        on lineup_players.athlete_id = athletes.athlete_id
    inner join weekly_periods
        on lineups.weekly_period_id = weekly_periods.weekly_period_id
    )

    select * from joined