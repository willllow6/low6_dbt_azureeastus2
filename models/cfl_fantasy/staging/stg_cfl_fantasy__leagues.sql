with source as (
    select * from {{ source('cfl_fantasy', 'leagues') }}
),

renamed as (
    select
        ---------- ids
        id as contest_id,
        leagueCode as league_code,
        'cfl' as client_id,
        'cfl' as tenant_id,

        ---------- strings
        'CFL' as tenant_name,
        'fantasy' as game_type,
        name as contest_name,
        draftType as draft_type,
        setupStep as setup_step,
        tradeApproval as trade_approval,

        ---------- numerics
        size as max_teams,
        selectionTime as selection_time_seconds,
        playoffTeams as playoff_teams,
        tradeDeadline as trade_deadline_week,
        cast(null as number) as entry_fee,
        cast(null as number) as prize_pool,

        ---------- booleans
        isPrivate as is_private,
        autoFill as is_auto_fill,
        isSetupComplete as is_setup_complete,
        isSetupComplete as is_active,
        case when isSetupComplete then 'active' else 'draft' end as contest_status,

        ---------- dates
        draftDay as draft_day,

        ---------- timestamps
        convert_timezone('UTC', draftStartTime)::timestamp_ntz as starts_at,
        convert_timezone('UTC', draftStartTime)::timestamp_ntz as draft_starts_at,
        cast(null as timestamp_ntz) as ends_at,
        convert_timezone('UTC', createdAt)::timestamp_ntz as created_at

    from source
)

select * from renamed
