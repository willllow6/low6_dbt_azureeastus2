with

users as (

    select *
    from {{ ref('dim_olybet_casino__users') }}

)

select
    user_id,
    registration_date,
    registration_date_et,
    registered_at,
    registered_at_et
from users
