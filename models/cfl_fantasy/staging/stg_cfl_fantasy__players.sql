with source as (
    select * from {{ source('cfl_fantasy', 'players') }}
),

renamed as (
    select
        ---------- ids
        id as player_id,
        externalId as external_player_id,
        sportKey as sport_key,
        teamExternalId as team_external_id,

        ---------- strings
        fullName as full_name,
        firstName as first_name,
        lastName as last_name,
        position,
        teamCode as team_code,
        countryCode as country_code,
        nationality,
        status as player_status,

        ---------- numerics
        jerseyNumber as jersey_number,

        ---------- booleans
        isNational as is_national,
        isCanadian as is_canadian,
        platformStatus as is_platform_active,

        ---------- variants
        metadata,
        updatedMetadata as updated_metadata,

        ---------- timestamps
        convert_timezone('UTC', createdAt)::timestamp_ntz as created_at

    from source
)

select * from renamed
