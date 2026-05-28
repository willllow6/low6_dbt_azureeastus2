with source as (
    select * from {{ source('cfl_fantasy', 'rosters') }}
),

renamed as (
    select
        ---------- ids
        id as roster_id,
        leagueId as contest_id,
        teamId as entry_id,
        playerId as player_id,
        'cfl' as client_id,
        'cfl' as tenant_id,

        ---------- strings
        'CFL' as tenant_name,
        'fantasy' as game_type,
        slotKey as slot_key,

        ---------- booleans
        bench as is_bench,

        ---------- timestamps
        convert_timezone('UTC', lockedUntil)::timestamp_ntz as locked_until,
        convert_timezone('UTC', createdAt)::timestamp_ntz as created_at

    from source
)

select * from renamed
