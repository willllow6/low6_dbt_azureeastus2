select
    user_id,
    penn_user_id,
    tier,
    platform,
    registration_type,
    registration_date,
    registration_date_et,
    registered_at,
    registered_at_et,
    updated_at
from {{ ref('dim_penn__users') }}
