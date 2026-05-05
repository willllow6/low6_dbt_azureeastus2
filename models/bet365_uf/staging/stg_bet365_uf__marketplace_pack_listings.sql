with

source as (

    select *
    from {{ source('bet365_uf', 'MARKETPLACE_SCOREABLE_PACKS') }}

),

renamed as (

    select

        ----------  ids
        MarketplaceScoreablePackId as pack_listing_id,
        TenantId as tenant_id,
        'bet365' as client_id,

        ---------- strings
        Name as pack_name,
        Description as pack_description,
        Image as pack_image,
        PackType as pack_type,
        PackSettings as pack_contents_settings,

        ---------- numerics
        PackOrder as pack_listing_order,
        Price as pack_price,
        Quantity as number_of_packs_sold,
        CardQuantity as number_of_cards_in_pack,
        UnlocksXpLevelId as unlocks_at_xp_level_id,

        ---------- dates
        cast(PublishDate as date) as pack_listing_published_date,
        cast(ExpiryDate as date) as pack_listing_expiry_date,
        cast(CreatedAt as date) as pack_listing_created_date,

        ---------- timestamps
        PublishDate as pack_listing_published_at,
        ExpiryDate as pack_listing_expires_at,
        CreatedAt as pack_listing_created_at

    from source

)

select * from renamed
