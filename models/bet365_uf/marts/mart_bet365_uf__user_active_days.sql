with

user_active_days as (

    select *
    from {{ ref('fct_bet365_uf__user_active_days') }}

),

users as (

    select
        user_id,
        is_tester
    from {{ ref('stg_bet365_uf__users') }}

),

joined as (

    select
        uad.user_id,
        uad.active_date,
        uad.user_active_day_number,
        u.is_tester
    from user_active_days as uad
    left join users as u
        on uad.user_id = u.user_id

)

select * from joined where is_tester = false