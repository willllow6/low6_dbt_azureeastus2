{{ config(
    materialized='view',
    secure=true,
    post_hook="{{ share_view('analytics_oilers_picks', 'shared_oilers_picks__users', 'oilers_share') }}"
) }}

-- Client contract: column names, types and order are fixed (see
-- _schema_oilers_picks_sharing.yml).

with

users as (

    select *
    from {{ ref('dim_oilers_picks__users') }}

)

select
    cast(user_id as number(38, 0)) as user_id,
    cast(service_user_id as number(38, 0)) as service_user_id,
    cast(favorite_team_id as number(38, 0)) as favorite_team_id,

    email,
    username as low6_username,
    first_name,
    last_name,
    postal_code,
    mobile_number,

    marketing_consent as has_consented_marketing,
    has_placed_sports_bet,

    cast(registered_at as date) as created_date,
    registration_date_et as created_date_et,

    registered_at as created_at,
    registered_at_et as created_at_et

from users
