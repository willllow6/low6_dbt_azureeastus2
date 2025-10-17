{{ config(
    materialized='view',
    secure=true,
    post_hook="{{ share_view('analytics_betway_picks', 'shared_betway_picks__aggregate_leaderboard_positions', 'betway_share') }}"
) }}

with

agg_leaderboard_positions as (

    select * from {{ ref('betway_picks__aggregate_leaderboard_positions') }}

)

select * from agg_leaderboard_positions