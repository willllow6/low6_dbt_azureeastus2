select
    user_id,
    sso_user_id,
    username,
    registration_date,
    registration_date_et,
    registered_at,
    registered_at_et
from {{ ref('dim_penn__users') }}
