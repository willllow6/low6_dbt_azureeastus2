with

users as (

    select *
    from {{ ref('stg_penn__users') }}
    where sso_user_id is not null

)

select
    user_id,
    sso_user_id,
    username,
    registration_date,
    cast(convert_timezone('UTC', '{{ var("local_timezone") }}', registered_at) as date) as registration_date_et,
    registered_at,
    convert_timezone('UTC', '{{ var("local_timezone") }}', registered_at)::timestamp_ntz as registered_at_et
from users
