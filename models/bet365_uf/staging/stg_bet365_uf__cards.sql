with

source as (

    select *
    from {{ source('bet365_uf', 'CARDS') }}

),

renamed as (

    select

        ----------  ids
        CardId as card_id,
        UserId as user_id,
        ScoreableId as scoreable_id,
        PackId as pack_id,
        TenantId as tenant_id,
        'bet365' as client_id,

        ---------- numerics
        Rating as card_rating,

        ---------- booleans
        IsSquad as is_squad,

        ---------- dates
        cast(AssignedAt as date) as card_acquired_date,
        cast(CreatedAt as date) as card_created_date,

        ---------- timestamps
        LockedForTradeExpiry as card_locked_for_trade_expiry,
        AssignedAt as card_acquired_at,
        CreatedAt as card_created_at

    from source

)

select * from renamed
