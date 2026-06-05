with

users as (

    select *
    from {{ ref('stg_opap_spintowin__users') }}

)

select
    user_id,
    sso_user_id,
    user_state,
    segment,
    registration_date,
    registration_date_et,
    registered_at,
    registered_at_et,
    last_seen_at
from users
