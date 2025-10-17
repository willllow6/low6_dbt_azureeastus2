with

store_purchases as (

    select *
    from {{ ref('stg_pln_arcade__store_purchases') }}

),

store_items as (

    select *
    from {{ ref('stg_pln_arcade__marketplace_store_items') }}

),

users as (

    select *
    from {{ ref('stg_pln_arcade__users') }}

),

rank_user_purchases as (

    select
        *,
        row_number() over (partition by user_id order by store_purchase_id) as user_purchase_number
    from store_purchases

),

joined as (

    select
        store_purchases.store_purchase_id,
        store_purchases.store_id,
        store_purchases.product_id,
        store_purchases.user_id,
        users.username,
        users.email,
        store_purchases.app_store,
        store_purchases.price,
        store_items.item_name,
        store_items.coin_amount,
        store_items.item_price,
        store_items.item_price_label,
        store_purchases.user_purchase_number,
        store_purchases.purchase_id,
        store_purchases.purchased_date,
        store_purchases.purchased_date_et,
        store_purchases.purchased_at,
        store_purchases.purchased_at_et
    from rank_user_purchases as store_purchases
    left join users
        on store_purchases.user_id = users.user_id
    left join store_items
        on store_purchases.store_id = store_items.store_id
        and store_purchases.product_id = store_items.product_id

)

select * from joined