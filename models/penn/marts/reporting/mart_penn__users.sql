select
    user_id,
    penn_user_id,
    tier,
    platform,
    registration_date,
    registration_date_et,
    registered_at,
    registered_at_et
from {{ ref('dim_penn__users') }}
