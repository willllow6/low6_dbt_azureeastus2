{{ config(
    materialized='view',
    secure=true,
    post_hook="{{ share_view('analytics_oilers_picks', 'shared_oilers_picks__leaderboard_positions', 'oilers_share') }}"
) }}

-- Client contract: column names, types and order are fixed (see
-- _schema_oilers_picks_sharing.yml). leaderboard_id is the period leaderboard
-- id and is null on contest leaderboards.

with

leaderboard_positions as (

    select *
    from {{ ref('fct_oilers_picks__leaderboard_positions') }}

),

users as (

    select *
    from {{ ref('dim_oilers_picks__users') }}

)

select
    cast(leaderboard_positions.period_leaderboard_id as number(38, 0)) as leaderboard_id,
    cast(leaderboard_positions.friend_league_id as number(38, 0)) as league_id,
    leaderboard_positions.leaderboard_name,
    leaderboard_positions.leaderboard_type,
    leaderboard_positions.leaderboard_starts_at,
    leaderboard_positions.leaderboard_ends_at,

    cast(leaderboard_positions.user_id as number(38, 0)) as user_id,
    users.username as low6_username,
    users.first_name,
    users.last_name,
    users.email,

    cast(leaderboard_positions.entries as number(18, 0)) as entries,
    cast(leaderboard_positions.total_points as number(38, 0)) as total_points,
    cast(leaderboard_positions.tiebreak_error as number(38, 0)) as tiebreak_error,
    cast(leaderboard_positions.leaderboard_position as number(18, 0)) as leaderboard_position

from leaderboard_positions
left join users
    on leaderboard_positions.user_id = users.user_id
