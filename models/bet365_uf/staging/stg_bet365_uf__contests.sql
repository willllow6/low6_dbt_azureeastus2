with

source as (

    select *
    from {{ source('bet365_uf', 'STAGES') }}

),

renamed as (

    select

        ----------  ids
        StageId as contest_id,
        TenantId as tenant_id,
        CompetitionId as competition_id,
        'bet365' as client_id,

        ---------- strings
        Name as contest_name,
        Identifier as contest_identifier,

        case
            when IsCompleted = true then 'completed'
            when sysdate() between StartDate and EndDate then 'live'
            else 'scheduled'
        end as contest_status,

        'fantasy' as game_type,

        ---------- numerics
        PrizePool as prize_pool,
        Breakdown as prize_breakdown,

        ---------- booleans
        IsCompleted as is_completed,
        IsPrizesPaid as is_prizes_paid,

        ---------- dates
        cast(StartDate as date) as contest_start_date,
        cast(EndDate as date) as contest_end_date,
        cast(CreatedAt as date) as created_date,

        ---------- timestamps
        CreatedAt as created_at,
        StartDate as starts_at,
        EndDate as ends_at

    from source

)

select * from renamed
