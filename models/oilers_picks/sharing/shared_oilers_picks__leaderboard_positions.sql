{{ config(
    materialized='view',
    secure=true,
    post_hook="{{ share_view('analytics_oilers_picks', 'shared_oilers_picks__leaderboard_positions', 'oilers_share') }}"
) }}

with

leaderboard_positions as (

    select * from {{ ref('oilers_picks__leaderboard_positions') }}

)

select * from leaderboard_positions