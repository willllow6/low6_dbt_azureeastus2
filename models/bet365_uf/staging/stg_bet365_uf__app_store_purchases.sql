with

source as (

    select *
    from {{ source('bet365_uf', 'STORE_PURCHASES') }}

),

renamed as (

    select

        ----------  ids
        StorePurchaseId as purchase_id,
        UserId as user_id,
        'bet365' as client_id,

        ---------- strings
        Store as app_store_id,
        TransactionId as app_store_transaction_id,
        ProductId as app_store_product_id,
        PurchaseId as app_store_purchase_id,
        case
            when Store = 0 then 'Apple'
            else 'Google'
        end as app_store_name,
        TransactionCurrency as purchase_currency,
        'fantasy' as game_type,

        ---------- numerics
        Price as purchase_price,

        ---------- dates
        cast(PurchaseDate as date) as purchased_date,
        cast(CreatedAt as date) as created_date,

        ---------- timestamps
        PurchaseDate as purchased_at,
        CreatedAt as created_at

    from source

)

select * from renamed
