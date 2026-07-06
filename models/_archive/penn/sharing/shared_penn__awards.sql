{{ config(
    materialized='view',
    secure=true,
    post_hook="{{ share_view('analytics_penn', 'shared_penn__awards', 'penn_share') }}"
) }}

select
    award_id,
    entry_id,
    user_id,
    penn_user_id,
    user_tier,
    contest_id,
    tenant_id,
    -- promotion_id,
    goal_id,
    player_external_id,
    event_external_id,
    outcome_id,
    prize_amount,
    currency,
    award_status,
    platform,
    delivered_at,
    delivered_date_et,
    created_at,
    updated_at
from {{ ref('mart_penn__awards') }}
