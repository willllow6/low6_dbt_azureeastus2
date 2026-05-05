with

purchases as (
    select * from {{ ref('fct_bet365_uf__app_store_purchases') }}
),

users as (
    select user_id, username, is_tester from {{ ref('stg_bet365_uf__users') }}
),

joined as (

    select
        purchases.purchase_id,
        purchases.user_id,
        purchases.client_id,
        purchases.game_type,
        purchases.app_store_id,
        purchases.app_store_transaction_id,
        purchases.app_store_product_id,
        purchases.app_store_purchase_id,
        purchases.app_store_name,
        purchases.purchase_currency,
        purchases.purchase_price,
        purchases.purchased_at,
        purchases.item_name,
        purchases.item_description,
        purchases.coins_amount,
        purchases.item_type,
        purchases.pack_type,
        purchases.user_purchase_number,
        users.username,
        users.is_tester
    from purchases
    left join users
        on purchases.user_id = users.user_id

)

select * from joined
