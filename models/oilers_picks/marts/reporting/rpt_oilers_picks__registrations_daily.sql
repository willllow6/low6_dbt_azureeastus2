with

users as (

    select *
    from {{ ref('dim_oilers_picks__users') }}

)

select
    registration_date_et,
    marketing_consent,

    client_id,
    tenant_id,
    tenant_name,
    game_type,

    count(*) as registrations

from users
group by 1, 2, 3, 4, 5, 6
