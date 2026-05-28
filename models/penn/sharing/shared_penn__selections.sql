{{ config(
    materialized='view',
    secure=true,
    post_hook="{{ share_view('analytics_penn', 'shared_penn__selections', 'penn_share') }}"
) }}

select
    selection_id,
    entry_id,
    user_id,
    penn_user_id,
    contest_id,
    tournament_id,
    tenant_id,
    game_type,
    contest_name,
    tournament_name,
    tenant_name,
    day_index,
    status,
    player_id,
    first_name,
    last_name,
    position,
    -- number,
    -- birth_country,
    revealed_rarity,
    revealed_at,
    revealed_at_et,
    reveal_start,
    reveal_end,
    created_at
from {{ ref('mart_penn__selections') }}
