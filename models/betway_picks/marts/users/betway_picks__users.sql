with

users as (
  
     select * from {{ ref('stg_betway_picks__users') }} 

),

leagues as (

    select *
    from {{ ref('stg_betway_picks__leagues') }}

),

joined as (

    select
        users.user_id,
        users.service_user_id,
        users.sso_user_id,
        users.betway_UserId,
        users.betway_CasinoId,
        users.betway_SubscriberKey,
        users.username,
        users.email,
        users.country,
        users.state,
        users.location,
        users.region,
        -- leagues.league_name as region,
        users.registration_date,
        users.registration_date_et,
        users.registered_at,
        users.registered_at_et
    from users
    left join leagues
        on users.location = leagues.league_code

)

select * from joined