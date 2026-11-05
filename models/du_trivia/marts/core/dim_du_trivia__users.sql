with

users as (

    select *
    from {{ ref('stg_du_trivia__users') }}

)

select
    user_id,
    service_user_id,
    external_user_id,

    client_id,
    tenant_id,
    tenant_name,
    game_type,

    registration_type,
    username,
    nickname,

    is_active,

    cast(
        convert_timezone('UTC', '{{ var("du_trivia_local_timezone") }}', registered_at) as date
    ) as registration_date_gst,

    registered_at,
    convert_timezone('UTC', '{{ var("du_trivia_local_timezone") }}', registered_at) as registered_at_gst,
    updated_at,
    deleted_at

from users
