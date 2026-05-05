with

purchases as (
    select * from {{ ref('stg_bet365_uf__app_store_purchases') }}
),

items as (
    select * from {{ ref('stg_bet365_uf__app_store_items') }}
),

final as (

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
        purchases.created_at,
        items.item_name,
        items.item_description,
        items.coins_amount,
        items.item_type,
        items.pack_type,
        row_number() over (
            partition by purchases.user_id
            order by purchases.purchased_at
        ) as user_purchase_number
    from purchases
    left join items
        on purchases.app_store_product_id = items.app_store_product_id
        and purchases.app_store_id = items.app_store_id

)

select * from final
