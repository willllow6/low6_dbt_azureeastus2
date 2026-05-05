with

source as (

    select *
    from {{ source('bet365_uf', 'MARKETPLACE_STORE_ITEMS') }}

),

renamed as (

    select

        ----------  ids
        MarketplaceStoreItemId as item_id,
        TenantId as tenant_id,
        'bet365' as client_id,

        ---------- strings
        Store as app_store_id,
        ProductId as app_store_product_id,
        Name as item_name,
        Description as item_description,
        PriceLabel as price_label,
        ItemType as item_type,
        PackType as pack_type,

        ---------- numerics
        Amount as coins_amount,
        round((Price / 100), 2) as purchase_price,

        ---------- dates
        cast(CreatedAt as date) as item_created_date,

        ---------- timestamps
        CreatedAt as item_created_at

    from source

)

select * from renamed
