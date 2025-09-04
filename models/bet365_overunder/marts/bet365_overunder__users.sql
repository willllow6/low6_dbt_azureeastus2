with

users as (

    select *
    from {{ ref('stg_bet365_overunder__users') }}

),

country_codes as (

    select *
    from {{ ref('iso_country_codes') }}

),

state_province_codes as (

    select *
    from {{ ref('state_province_codes') }}

),

joined as (

    select
        users.user_id,
        users.gaming_id,

        users.currency_code,
        users.country_code,
        users.state_code,
        users.segment_group,

        coalesce(country_codes.country,'Unknown') as country,
        coalesce(state_province_codes.state_province,'Unknown') as state_province,

        users.has_logged_in_since_launch,

        users.registration_date,
        users.registered_at,
        users.updated_at,
        users.last_login_at
    
    from users
    left join country_codes
        on users.country_code = country_codes.alpha_2_code
    left join state_province_codes
        on users.state_code = state_province_codes.code

)

select * from joined