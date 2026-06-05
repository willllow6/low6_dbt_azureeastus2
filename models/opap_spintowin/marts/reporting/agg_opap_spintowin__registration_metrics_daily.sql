with

users as (

    select *
    from {{ ref('dim_opap_spintowin__users') }}

),

daily as (

    select
        registration_date_et                as date_day,
        'opap'                              as client_id,
        'opap'                              as tenant_id,
        'OPAP'                              as tenant_name,
        'spin_to_win'                       as game_type,
        'sso'                               as registration_type,
        count(*)                            as new_registrations,
        0                                   as profile_completions,
        0                                   as marketing_consents
    from users
    group by 1, 2, 3, 4, 5, 6

)

select * from daily
