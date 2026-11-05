with

source as (

    select *
    from {{ source('du_trivia', 'questions') }}

),

renamed as (

    select

        ----------  ids
        id::varchar as question_id,
        contestid::varchar as contest_id,
        'du' as client_id,
        'du' as tenant_id,

        ---------- strings
        'DU' as tenant_name,
        'trivia' as game_type,
        title as question_title,
        titlear as question_title_ar,
        image as question_image,
        question_type,

        ---------- numerics
        duration as question_duration,
        points as question_points,

        ---------- timestamps
        convert_timezone('UTC', createdat)::timestamp_ntz as created_at,
        convert_timezone('UTC', updatedat)::timestamp_ntz as updated_at

    from source

)

select * from renamed
