{{ config(
    materialized='view',
    secure=true,
    post_hook="{{ share_view('analytics_penn', 'shared_penn__entries', 'penn_share') }}"
) }}

select
    entry_id,
    user_id,
    penn_user_id,
    contest_id,
    tournament_id,
    game_type,
    contest_name,
    tournament_name,
    contest_status,
    round_sequence,
    tenant_id,
    tenant_name,
    contest_starts_at,
    contest_starts_at_et,
    contest_start_date_et,
    user_tier,
    entry_status,
    per_goal_amount,
    per_goal_currency,
    user_entry_number,
    user_entry_type,
    entry_date,
    entered_at,
    entry_date_et,
    entry_day_et,
    entry_hour_et,
    entered_at_et,
    updated_at
from {{ ref('mart_penn__entries') }}
