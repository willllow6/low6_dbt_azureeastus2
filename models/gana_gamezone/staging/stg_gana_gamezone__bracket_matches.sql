with

source as (

    select * from {{ source('gana_gamezone', 'bracket_matches') }}

),

renamed as (

    select

        ---------- ids
        id                                  as match_id,

        ---------- strings
        type                                as round,

        ---------- numerics
        position                            as bracket_position,

        ---------- booleans
        winner is null                      as is_active,

        ---------- semi-structured
        countries,
        winner::varchar                     as correct_country_id,

        ---------- timestamps
        cast(created_at as timestamp_ntz)   as created_at,
        cast(updated_at as timestamp_ntz)   as updated_at

    from source

)

select * from renamed
