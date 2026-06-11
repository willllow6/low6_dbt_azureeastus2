with

source as (

    select * from {{ source('gana_gamezone', 'survivor_predictions') }}

),

renamed as (

    select

        ---------- ids
        id                                      as selection_id,
        user_id,
        survivor_match_id                       as contest_id,

        ---------- strings
        'gana'                                  as client_id,
        'gana'                                  as tenant_id,
        'Gana'                                  as tenant_name,
        'survivor'                              as game_type,
        -- source column is misnamed: correct_answer stores the user's pick, not the correct answer
        correct_answer::varchar                 as selected_country_id,

        ---------- timestamps
        cast(created_at as timestamp_ntz)       as selected_at,
        cast(updated_at as timestamp_ntz)       as updated_at

    from source

)

select * from renamed
