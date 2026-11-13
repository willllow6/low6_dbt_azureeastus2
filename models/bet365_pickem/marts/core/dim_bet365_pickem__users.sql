with

users as (

    select *
    from {{ ref('stg_bet365_pickem__users') }}

)

select
    user_id,
    external_user_id,

    client_id,
    tenant_id,
    tenant_name,
    game_type,

    registration_type,
    country_code,
    state_code,
    display_name,
    currency,
    language,
    price_format,

    cast(
        convert_timezone('UTC', '{{ var("local_timezone") }}', registered_at) as date
    ) as registration_date_et,

    registered_at,
    convert_timezone('UTC', '{{ var("local_timezone") }}', registered_at) as registered_at_et,
    last_launched_at

from users
