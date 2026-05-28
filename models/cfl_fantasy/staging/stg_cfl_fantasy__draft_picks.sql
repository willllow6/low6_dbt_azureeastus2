with source as (
    select * from {{ source('cfl_fantasy', 'draft_picks') }}
),

renamed as (
    select
        ---------- ids
        id as draft_pick_id,
        draftId as draft_id,
        teamId as team_id,
        playerId as player_id,
        'cfl' as client_id,
        'cfl' as tenant_id,

        ---------- strings
        'CFL' as tenant_name,
        'fantasy' as game_type,
        source as pick_source,

        ---------- numerics
        round as draft_round,
        pickNumber as pick_number,

        ---------- timestamps
        convert_timezone('UTC', pickedAt)::timestamp_ntz as picked_at

    from source
)

select * from renamed
