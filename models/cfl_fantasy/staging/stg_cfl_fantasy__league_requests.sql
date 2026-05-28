with source as (
    select * from {{ source('cfl_fantasy', 'league_requests') }}
),

renamed as (
    select
        ---------- ids
        id as league_request_id,
        leagueId as contest_id,
        userId as user_id,
        teamId as entry_id,
        imageId as image_id,
        'cfl' as client_id,
        'cfl' as tenant_id,

        ---------- strings
        'CFL' as tenant_name,
        'fantasy' as game_type,
        teamName as team_name,
        status as request_status,

        ---------- timestamps
        convert_timezone('UTC', createdAt)::timestamp_ntz as created_at,
        convert_timezone('UTC', updatedAt)::timestamp_ntz as updated_at

    from source
)

select * from renamed
