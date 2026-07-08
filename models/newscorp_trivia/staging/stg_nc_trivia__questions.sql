with

source as (

    select *
    from {{ source('nc_trivia', 'questions') }}

),

renamed as (

    select

        ----------  ids
        id as question_id,
        contestid as contest_id,

        ---------- strings
        title as question_title,
        description as question_description,
        question_type,

        ---------- numerics
        duration as question_duration,
        points as question_maximum_points,

        ---------- booleans

        ---------- dates

        ---------- timestamps
        createdat as created_at,
        updatedat as updated_at

    from source

)

select * from renamed