with source as (
    select * from {{ source('cfl_fantasy', 'waivers') }}
),

renamed as (
    select
        ---------- ids
        id as waiver_id,
        leagueId as contest_id,
        teamId as entry_id,
        playerId as player_id,
        dropPlayerId as drop_player_id,
        'cfl' as client_id,
        'cfl' as tenant_id,

        ---------- strings
        'CFL' as tenant_name,
        'fantasy' as game_type,
        status as waiver_status,

        ---------- numerics
        priority as waiver_priority,

        ---------- variants
        payload,

        ---------- timestamps
        convert_timezone('UTC', createdAt)::timestamp_ntz as created_at,
        convert_timezone('UTC', processedAt)::timestamp_ntz as processed_at

    from source
)

select * from renamed
