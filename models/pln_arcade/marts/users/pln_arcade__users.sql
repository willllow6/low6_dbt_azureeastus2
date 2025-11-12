with 

users as (

    select *
    from {{ ref('stg_pln_arcade__users') }}

),

coin_transactions as (

    select *
    from {{ ref('stg_pln_arcade__coin_transactions') }}

),

purchases as (

    select *
    from {{ ref('stg_pln_arcade__store_purchases') }}

),

coin_balances as (

    select
        user_id,
        sum(coin_amount) as coin_balance
    from coin_transactions
    group by user_id

),

user_purchases as (

    select
        user_id,
        sum(price) as total_gross_revenue
    from purchases
    group by 1

),

joined as (

    select
        users.user_id,
        users.username,
        users.email,
        users.date_of_birth,
        users.generation,
        users.age,
        users.age_band,
        users.is_enabled,
        users.has_consented_pln_marketing,
        coalesce(coin_balances.coin_balance,0) as coin_balance,
        coalesce(user_purchases.total_gross_revenue,0) as total_gross_revenue,
        case
            when total_gross_revenue is null then ''
            when total_gross_revenue <= 0 then ''
            when total_gross_revenue < 5 then '<£5'
            when total_gross_revenue < 10 then '£5-9.99'
            when total_gross_revenue < 25 then '£10-24.99'
            when total_gross_revenue < 50 then '£25-49.99'
            when total_gross_revenue < 75 then '£50-74.99'
            when total_gross_revenue < 100 then '£75-99.99'
            else '£100+'
        end as gross_revenue_band,
        users.user_created_date,
        users.user_created_date_et,
        users.user_created_at,
        users.user_created_at_et
    from users
    left join coin_balances
        on users.user_id = coin_balances.user_id
    left join user_purchases
        on users.user_id = user_purchases.user_id
    
)

select * from joined