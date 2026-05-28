with source as (
    select * from {{ source('cfl_fantasy', 'trades') }}
),

renamed as (
    select
        ---------- ids
        id as trade_id,
        leagueId as contest_id,
        proposerTeamId as proposer_entry_id,
        targetTeamId as target_entry_id,
        'cfl' as client_id,
        'cfl' as tenant_id,

        ---------- strings
        'CFL' as tenant_name,
        'fantasy' as game_type,
        status as trade_status,

        ---------- variants
        payload,

        ---------- timestamps
        convert_timezone('UTC', expiresAt)::timestamp_ntz as expires_at,
        convert_timezone('UTC', processedAt)::timestamp_ntz as processed_at,
        convert_timezone('UTC', createdAt)::timestamp_ntz as created_at

    from source
)

select * from renamed
