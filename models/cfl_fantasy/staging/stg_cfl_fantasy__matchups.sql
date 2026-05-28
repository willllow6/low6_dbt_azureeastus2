with source as (
    select * from {{ source('cfl_fantasy', 'matchups') }}
),

renamed as (
    select
        ---------- ids
        id as matchup_id,
        leagueId as contest_id,
        homeTeamId as home_entry_id,
        awayTeamId as away_entry_id,
        'cfl' as client_id,
        'cfl' as tenant_id,

        ---------- strings
        'CFL' as tenant_name,
        'fantasy' as game_type,
        status as matchup_status,

        ---------- numerics
        week as fantasy_week,

        ---------- variants
        result,

        ---------- timestamps
        convert_timezone('UTC', createdAt)::timestamp_ntz as created_at

    from source
)

select * from renamed
