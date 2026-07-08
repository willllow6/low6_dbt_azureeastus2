with

source as (

    select *
    from {{ source('nc_trivia', 'options') }}

),

renamed as (

    select

        ----------  ids
        id as option_id,
        question_id,

        ---------- strings
        title as option_title,
        description as option_description,

        ---------- numerics

        ---------- booleans
        is_correct,

        ---------- dates

        ---------- timestamps
        createdat as created_at,
        updatedat as updated_at

    from source

)

select * from renamed