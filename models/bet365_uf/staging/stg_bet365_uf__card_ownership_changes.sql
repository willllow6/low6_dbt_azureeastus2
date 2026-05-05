with

source as (

    select *
    from {{ source('bet365_uf', 'CARD_OWNERSHIP_HISTORY') }}

),

renamed as (

    select

        ----------  ids
        CardOwnershipHistoryId as card_ownership_change_id,
        REGEXP_REPLACE(Reference, '[^0-9]') as coin_transaction_id,
        TenantId as tenant_id,
        CardId as card_id,
        ScoreableId as scoreable_id,
        FromUserId as card_ownership_changed_from_user_id,
        ToUserId as card_ownership_changed_to_user_id,
        'bet365' as client_id,

        ---------- strings
        Event as card_ownership_change_event,
        Reference as card_ownership_change_reference,

        ---------- numerics
        Price as coin_amount,

        ---------- dates
        cast(CreatedAt as date) as card_ownership_change_date,

        ---------- timestamps
        CreatedAt as card_ownership_changed_at

    from source

)

select * from renamed
