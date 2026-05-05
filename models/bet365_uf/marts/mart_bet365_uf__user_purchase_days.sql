with

user_purchase_days as (

    select *
    from {{ ref('fct_bet365_uf__user_purchase_days') }}

),

users as (

    select
        user_id,
        is_tester
    from {{ ref('stg_bet365_uf__users') }}

),

joined as (

    select
        upd.user_id,
        upd.purchased_date,
        u.is_tester
    from user_purchase_days as upd
    left join users as u
        on upd.user_id = u.user_id

)

select * from joined where is_tester = false