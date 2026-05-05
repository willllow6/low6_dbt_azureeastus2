with

contests as (
    select * from {{ ref('stg_bet365_uf__contests') }}
),

game_weeks as (
    select * from {{ ref('stg_bet365_uf__game_weeks') }}
),

competitions as (
    select * from {{ ref('stg_bet365_uf__competitions') }}
),

contest_context as (

    select
        contests.contest_id,
        contests.tenant_id,
        contests.competition_id,
        contests.client_id,
        contests.game_type,
        contests.contest_name,
        contests.contest_identifier,
        contests.contest_status,
        contests.starts_at,
        contests.ends_at,
        contests.contest_start_date,
        contests.contest_end_date,
        contests.prize_pool,
        contests.is_completed,
        contests.is_prizes_paid,
        contests.created_at,
        game_weeks.stage_period_id as game_week_id,
        game_weeks.game_week_name,
        game_weeks.game_week_description,
        game_weeks.game_week_start_date,
        game_weeks.game_week_end_date,
        game_weeks.game_week_starts_at,
        game_weeks.game_week_ends_at,
        game_weeks.is_game_week_complete,
        competitions.competition_name,
        competitions.is_international_competition
    from contests
    inner join game_weeks
        on contests.starts_at >= game_weeks.game_week_starts_at
        and contests.starts_at <= game_weeks.game_week_ends_at
        and contests.tenant_id = game_weeks.tenant_id
        and contests.competition_id = game_weeks.competition_id
    left join competitions
        on contests.competition_id = competitions.competition_id

)

select * from contest_context
