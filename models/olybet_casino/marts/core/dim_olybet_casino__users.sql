with

users as (

    select *
    from {{ ref('int_olybet_casino__users') }}

)

select
    user_id,
    registration_date,
    cast(convert_timezone('UTC', '{{ var("olybet_casino_local_timezone") }}', registered_at) as date) as registration_date_et,
    registered_at,
    convert_timezone('UTC', '{{ var("olybet_casino_local_timezone") }}', registered_at)::timestamp_ntz as registered_at_et
from users
