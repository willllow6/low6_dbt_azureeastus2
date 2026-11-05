with

source as (

    select *
    from {{ source('du_trivia', 'options') }}

),

renamed as (

    select

        ----------  ids
        id::varchar as option_id,
        question_id::varchar as question_id,
        'du' as client_id,
        'du' as tenant_id,

        ---------- strings
        'DU' as tenant_name,
        'trivia' as game_type,
        title as option_title,
        titlear as option_title_ar,

        ---------- booleans
        is_correct,

        ---------- timestamps
        convert_timezone('UTC', createdat)::timestamp_ntz as created_at,
        convert_timezone('UTC', updatedat)::timestamp_ntz as updated_at

    from source

)

select * from renamed
