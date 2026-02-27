with

store_purchases as (

    select *
    from {{ ref('stg_pln_arcade__coin_transactions') }}
    where transaction_type = 5

),

users as (

    select *
    from {{ ref('stg_pln_arcade__users') }}

),

rank_user_purchases as (

    select
        *,
        row_number() over (partition by user_id order by coin_transaction_id) as user_purchase_number
    from store_purchases

),

joined as (

    select
        store_purchases.coin_transaction_id,
        -- store_purchases.store_id,
        -- store_purchases.product_id,
        store_purchases.user_id,
        users.username,
        users.email,
        'Apple' as app_store,
        0.99 as price,
        '100 Coins' as item_name,
        100 as coin_amount,
        0.99 as item_price,
        '0.99' as item_price_label,
        store_purchases.user_purchase_number,
        store_purchases.created_date as purchased_date,
        store_purchases.created_date_et as purchased_date_et,
        store_purchases.created_at as purchased_at,
        store_purchases.created_at_et as purchased_at_et
    from rank_user_purchases as store_purchases
    left join users
        on store_purchases.user_id = users.user_id

)

select * from joined