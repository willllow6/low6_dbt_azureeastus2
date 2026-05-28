with source as (
    select * from {{ source('cfl_fantasy', 'scores') }}
),

renamed as (
    select
        ---------- ids
        id as score_id,
        leagueId as contest_id,
        teamId as entry_id,
        'cfl' as client_id,
        'cfl' as tenant_id,

        ---------- strings
        'CFL' as tenant_name,
        'fantasy' as game_type,

        ---------- numerics
        week as fantasy_week,
        points,

        ---------- variants
        breakdown,

        ---------- timestamps
        convert_timezone('UTC', createdAt)::timestamp_ntz as created_at,
        convert_timezone('UTC', updatedAt)::timestamp_ntz as updated_at

    from source
)

select * from renamed
