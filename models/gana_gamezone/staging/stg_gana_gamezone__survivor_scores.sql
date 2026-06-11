with

source as (

    select * from {{ source('gana_gamezone', 'survivor_scores') }}

),

renamed as (

    select

        ---------- ids
        id                                      as score_id,
        user_id,
        survivor_match_id                       as contest_id,
        survivor_prediction_id                  as selection_id,

        ---------- strings
        'gana'                                  as client_id,
        'gana'                                  as tenant_id,
        'Gana'                                  as tenant_name,
        'survivor'                              as game_type,

        ---------- numerics
        current_streak,
        best_streak,

        ---------- booleans
        is_correct,

        ---------- timestamps
        cast(created_at as timestamp_ntz)       as created_at,
        cast(updated_at as timestamp_ntz)       as updated_at

    from source

)

select * from renamed
