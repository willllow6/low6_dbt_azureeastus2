with

source as (

    select *
    from {{ source('bet365_uf', 'MARKETPLACE_CARDS') }}

),

renamed as (

    select

        ----------  ids
        MarketplaceCardId as card_listing_id,
        TenantId as tenant_id,
        UserId as user_id,
        CardId as card_id,
        'bet365' as client_id,

        ---------- numerics
        Price as listing_price,

        ---------- strings
        Metadata as listing_meta_data,

        ---------- dates
        cast(ExpiresAt as date) as listing_expiry_date,
        cast(CreatedAt as date) as listing_created_date,

        ---------- timestamps
        ExpiresAt as listing_expires_at,
        CreatedAt as listing_created_at

    from source

)

select * from renamed
