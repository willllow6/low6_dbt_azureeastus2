with

scoreables as (
    select * from {{ ref('stg_bet365_uf__scoreables') }}
),

scoreable_stats as (
    select * from {{ ref('stg_bet365_uf__scoreable_fixture_stats') }}
),

fixtures as (
    select * from {{ ref('stg_bet365_uf__fixtures') }}
),

contests as (
    select * from {{ ref('stg_bet365_uf__contests') }}
),

fixture_with_contest as (

    select
        fixtures.*,
        contests.contest_name
    from fixtures
    inner join contests
        on fixtures.fixture_start_date >= contests.contest_start_date
        and fixtures.fixture_start_date <= contests.contest_end_date

),

scored_scoreables as (

    select
        scoreables.scoreable_id,
        scoreables.team_scoreable_id,
        scoreables.scoreable_external_reference,
        scoreables.scoreable_name,
        scoreables.scoreable_team_name,
        scoreables.scoreable_rating,
        scoreables.scoreable_position,
        scoreables.is_active,
        scoreables.is_injured,
        scoreable_stats.player_goals,
        scoreable_stats.player_assists,
        scoreable_stats.player_shots,
        scoreable_stats.player_hits,
        scoreable_stats.player_blocked_shots,
        scoreable_stats.team_win,
        scoreable_stats.team_power_play_goals,
        scoreable_stats.team_short_handed_goals,
        scoreable_stats.team_goals_against,
        scoreable_stats.team_shutout,
        scoreable_stats.team_saves,
        scoreable_stats.season,
        scoreable_stats.fixture_id,
        coalesce(scoreable_stats.team_win, 0) * 3 as team_win_points,
        coalesce(scoreable_stats.team_power_play_goals, 0) * 1 as team_power_play_goals_points,
        coalesce(scoreable_stats.team_short_handed_goals, 0) * 2 as team_short_handed_goals_points,
        coalesce(scoreable_stats.team_goals_against, 0) * -3 as team_goals_against_points,
        coalesce(scoreable_stats.team_shutout, 0) * 5 as team_shutout_points,
        coalesce(scoreable_stats.team_saves, 0) * 1 as team_saves_points,
        coalesce(scoreable_stats.player_goals, 0) * 7 as player_goals_points,
        coalesce(scoreable_stats.player_assists, 0) * 4 as player_assists_points,
        coalesce(scoreable_stats.player_shots, 0) * 1 as player_shots_points,
        coalesce(scoreable_stats.player_hits, 0) * 1 as player_hits_points,
        coalesce(scoreable_stats.player_blocked_shots, 0) * 1 as player_blocked_shots_points,
        case
            when scoreables.scoreable_position = 'Team'
                then
                    coalesce(scoreable_stats.team_win, 0) * 3 +
                    coalesce(scoreable_stats.team_power_play_goals, 0) * 1 +
                    coalesce(scoreable_stats.team_short_handed_goals, 0) * 2 +
                    coalesce(scoreable_stats.team_goals_against, 0) * -3 +
                    coalesce(scoreable_stats.team_shutout, 0) * 5 +
                    coalesce(scoreable_stats.team_saves, 0) * 1
            else
                coalesce(scoreable_stats.player_goals, 0) * 7 +
                coalesce(scoreable_stats.player_assists, 0) * 4 +
                coalesce(scoreable_stats.player_shots, 0) * 1 +
                coalesce(scoreable_stats.player_hits, 0) * 1 +
                coalesce(scoreable_stats.player_blocked_shots, 0) * 1
        end as total_points
    from scoreables
    inner join scoreable_stats
        on scoreables.scoreable_id = scoreable_stats.scoreable_id

),

final as (

    select
        scored_scoreables.*,
        fixture_with_contest.contest_name,
        fixture_with_contest.fixture_name
    from scored_scoreables
    inner join fixture_with_contest
        on scored_scoreables.fixture_id = fixture_with_contest.fixture_id

)

select * from final
