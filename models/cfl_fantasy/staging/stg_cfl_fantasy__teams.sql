with source as (
    select * from {{ source('cfl_fantasy', 'teams') }}
),

renamed as (
    select
        ---------- ids
        id as entry_id,
        leagueId as contest_id,
        userId as user_id,
        'cfl' as client_id,
        'cfl' as tenant_id,

        ---------- strings
        'CFL' as tenant_name,
        'fantasy' as game_type,
        name as team_name,
        imageId as image_id,

        ---------- numerics
        waiverPriority as waiver_priority,

        ---------- booleans
        isCommissioner as is_commissioner,
        preDraftAutoDraft as is_auto_draft,
        isAutoTeam as is_auto_team,

        ---------- variants
        preDraftQueue as pre_draft_queue,

        ---------- timestamps
        convert_timezone('UTC', createdAt)::timestamp_ntz as entered_at,
        convert_timezone('UTC', updatedAt)::timestamp_ntz as updated_at

    from source
)

select * from renamed
