{{ config(
    materialized='view',
    secure=true,
    post_hook="{{ share_view('analytics_penn', 'shared_penn__daily_reveal_selections', 'penn_share') }}"
) }}

select
    user_card_id,
    entry_id,
    user_id,
    sso_user_id,
    contest_id,
    day_number,
    game_date,
    player_name,
    first_name,
    last_name,
    position,
    team_name,
    team_abbreviation,
    sport,
    card_rating,
    total_hits,
    points,
    selected_at,
    selected_at_et,
    created_at
from {{ ref('mart_penn__daily_reveal_selections') }}
