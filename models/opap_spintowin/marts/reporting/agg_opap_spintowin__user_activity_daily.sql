with

entries as (

    select *
    from {{ ref('fct_opap_spintowin__entries') }}

),

users as (

    select
        user_id,
        registration_date_et
    from {{ ref('dim_opap_spintowin__users') }}

),

dau as (

    select
        entry_date_et                                                       as date_day,
        'opap'                                                              as client_id,
        'opap'                                                              as tenant_id,
        'OPAP'                                                              as tenant_name,
        'spin_to_win'                                                       as game_type,
        count(distinct user_id)                                             as dau,
        count(distinct case when user_entry_number = 1 then user_id end)    as first_time_entrants
    from entries
    group by 1, 2, 3, 4, 5

),

registrations as (

    select
        registration_date_et    as date_day,
        count(*)                as new_registrations
    from users
    group by 1

),

joined as (

    select
        d.date_day,
        d.client_id,
        d.tenant_id,
        d.tenant_name,
        d.game_type,
        d.dau,
        coalesce(r.new_registrations, 0)        as new_registrations,
        d.dau - d.first_time_entrants           as returning_users
    from dau d
    left join registrations r
        on d.date_day = r.date_day

)

select * from joined
