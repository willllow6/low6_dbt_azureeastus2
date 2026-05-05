with

source as (

    select *
    from {{ source('bet365_uf', 'LINEUPS') }}

),

renamed as (

    select

        ----------  ids
        LineupId as entry_id,
        StageId as contest_id,
        TenantId as tenant_id,
        UserId as user_id,
        'bet365' as client_id,

        ---------- strings
        'fantasy' as game_type,

        ---------- numerics
        Score as entry_score,

        ---------- booleans

        ---------- dates
        cast(CreatedAt as date) as entry_date,

        ---------- timestamps
        PaidPrizeAt as prize_paid_at,
        CreatedAt as entered_at

    from source

)

select * from renamed
