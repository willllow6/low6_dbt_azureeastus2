with source as (
    select * from {{ source('cfl_fantasy', 'sport_teams') }}
),

renamed as (
    select
        ---------- ids
        id as sport_team_id,
        externalId as external_team_id,
        sportKey as sport_key,

        ---------- strings
        name as team_name,
        abbreviation as team_abbreviation,
        teamShortName as team_short_name,
        teamCityName as team_city_name,
        image as team_image,

        ---------- variants
        metadata,

        ---------- timestamps
        convert_timezone('UTC', createdAt)::timestamp_ntz as created_at,
        convert_timezone('UTC', updatedAt)::timestamp_ntz as updated_at

    from source
)

select * from renamed
