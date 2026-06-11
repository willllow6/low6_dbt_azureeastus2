with

source as (

    select * from {{ source('gana_gamezone', 'group_predictions') }}

),

renamed as (

    select

        ---------- ids
        id                          as entry_id,
        user_id,

        ---------- strings
        'gana'                      as client_id,
        'gana'                      as tenant_id,
        'Gana'                      as tenant_name,
        'gana_predictor'            as contest_id,
        'predictor'                 as game_type,

        ---------- semi-structured
        prediction,

        ---------- timestamps
        cast(created_at as timestamp_ntz)   as entered_at,
        cast(updated_at as timestamp_ntz)   as updated_at

    from source

)

select * from renamed
