with

source as (

    select *
    from {{ source('bet365_uf', 'STAGE_PERIODS') }}

),

renamed as (

    select

        ----------  ids
        StagePeriodId as stage_period_id,
        TenantId as tenant_id,
        CompetitionId as competition_id,
        'bet365' as client_id,

        ---------- strings
        Name as game_week_name,
        Description as game_week_description,
        PrizeBreakdown as prize_structure,

        ---------- booleans
        IsPaid as is_game_week_paid,
        IsCompleted as is_game_week_complete,

        ---------- dates
        cast(CreatedAt as date) as game_week_created_date,
        cast(StartDate as date) as game_week_start_date,
        cast(EndDate as date) as game_week_end_date,

        ---------- timestamps
        CreatedAt as game_week_created_at,
        StartDate as game_week_starts_at,
        EndDate as game_week_ends_at

    from source

)

select * from renamed
