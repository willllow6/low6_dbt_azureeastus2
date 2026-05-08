{{ config(
    materialized='view',
    secure=true,
    post_hook="{{ share_view('analytics_penn', 'shared_penn__entries', 'penn_share') }}"
) }}

select
    entry_id,
    user_id,
    sso_user_id,
    contest_id,
    -- username,
    -- registration_date_et,
    contest_title,
    game_type,
    contest_status,
    -- contest_prize,
    contest_start_date,
    contest_starts_at,
    contest_start_date_et,
    contest_starts_at_et,
    points,
    days_entered,
    -- user_entry_number,
    -- user_entry_type,
    entry_date,
    -- entry_day,
    -- entry_hour,
    entered_at,
    entry_date_et,
    -- entry_day_et,
    -- entry_hour_et,
    entered_at_et,
    updated_at
from {{ ref('mart_penn__entries') }}
