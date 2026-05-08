{{ config(
    materialized='view',
    secure=true,
    post_hook="{{ share_view('analytics_penn', 'shared_penn__users', 'penn_share') }}"
) }}

select
    user_id,
    sso_user_id,
    -- username,
    registration_date,
    registration_date_et,
    registered_at,
    registered_at_et
from {{ ref('mart_penn__users') }}
