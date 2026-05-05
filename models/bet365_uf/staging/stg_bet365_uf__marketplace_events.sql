with

source as (

    select *
    from {{ source('bet365_uf', 'MARKETPLACE_USER_ACTIVITY') }}

),

renamed as (

    select

        ----------  ids
        MarketplaceUserActivityId as marketplace_event_id,
        TenantId as tenant_id,
        UserId as user_id,
        'bet365' as client_id,

        ---------- strings
        ActivityType as marketplace_event_type,
        RelatedType as marketplace_event_related_type,
        RelatedId as marketplace_event_related_id,

        ---------- dates
        cast(CreatedAt as date) as marketplace_event_created_date,

        ---------- timestamps
        CreatedAt as marketplace_event_created_at

    from source

)

select * from renamed
