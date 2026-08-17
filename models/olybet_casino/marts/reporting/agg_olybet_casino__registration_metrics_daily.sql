with

users as (

    select *
    from {{ ref('dim_olybet_casino__users') }}

),

daily as (

    select
        registration_date_et                as date_day,
        'olybet'                            as client_id,
        'olybet'                            as tenant_id,
        'Olybet'                            as tenant_name,
        'instant_win'                       as game_type,
        cast(null as varchar)               as registration_type,
        count(*)                            as new_registrations,
        0                                   as profile_completions,
        0                                   as marketing_consents
    from users
    group by 1, 2, 3, 4, 5, 6

)

select * from daily
