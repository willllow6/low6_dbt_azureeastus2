{{ config(
    materialized='view',
    secure=true,
    post_hook="{{ share_view('analytics_betway_picks', 'shared_betway_picks__contest_leaderboard_positions', 'betway_share') }}"
) }}

with

contest_leaderboards as (

    select * from {{ ref('betway_picks__contest_leaderboard_positions') }}

)

select * from contest_leaderboards