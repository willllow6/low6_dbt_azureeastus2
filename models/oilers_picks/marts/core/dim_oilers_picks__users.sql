with

users as (

    select *
    from {{ ref('stg_oilers_picks__users') }}

)

select
    user_id,
    service_user_id,
    external_user_id,
    favorite_team_id,

    client_id,
    tenant_id,
    tenant_name,
    game_type,

    registration_type,
    username,
    email,
    first_name,
    last_name,
    postal_code,
    mobile_number,

    marketing_consent,
    has_placed_sports_bet,

    cast(
        convert_timezone('UTC', '{{ var("local_timezone") }}', registered_at) as date
    ) as registration_date_et,

    registered_at,
    convert_timezone('UTC', '{{ var("local_timezone") }}', registered_at) as registered_at_et

from users
