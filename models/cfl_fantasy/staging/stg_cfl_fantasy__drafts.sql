with source as (
    select * from {{ source('cfl_fantasy', 'drafts') }}
),

renamed as (
    select
        ---------- ids
        id as draft_id,
        leagueId as contest_id,
        'cfl' as client_id,
        'cfl' as tenant_id,

        ---------- strings
        'CFL' as tenant_name,
        'fantasy' as game_type,
        type as draft_type,
        status as draft_status,

        ---------- variants
        state as draft_state,

        ---------- timestamps
        convert_timezone('UTC', startedAt)::timestamp_ntz as started_at,
        convert_timezone('UTC', endedAt)::timestamp_ntz as ended_at,
        convert_timezone('UTC', createdAt)::timestamp_ntz as created_at

    from source
)

select * from renamed
