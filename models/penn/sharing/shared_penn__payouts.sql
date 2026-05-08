{{ config(
    materialized='view',
    secure=true,
    post_hook="{{ share_view('analytics_penn', 'shared_penn__payouts', 'penn_share') }}"
) }}

select
    payout_id,
    user_id,
    sso_user_id,
    contest_id,
    prize_type,
    prize_amount,
    payment_status,
    payment_processed_at,
    created_at,
    updated_at
from {{ ref('mart_penn__payouts') }}
