with

source as (

    select * from {{ source('gana_gamezone', 'bracket_predictions') }}

),

renamed as (

    select

        ---------- ids
        id                                      as selection_id,
        user_id,
        bracket_match_id                        as match_id,

        ---------- strings
        'gana_bracket'                          as contest_id,
        'gana'                                  as client_id,
        'gana'                                  as tenant_id,
        'Gana'                                  as tenant_name,
        'bracket'                               as game_type,
        -- source column is misnamed: winner stores the user's pick, not the match winner
        winner::varchar                         as selected_country_id,

        ---------- timestamps
        cast(created_at as timestamp_ntz)       as selected_at,
        cast(updated_at as timestamp_ntz)       as updated_at

    from source

)

select * from renamed
