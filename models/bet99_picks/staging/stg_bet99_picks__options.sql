with

source as (

    select *
    from {{ source('bet99_picks', 'options') }}

),

renamed as (

    select

        ----------  ids
        id as option_id,
        question_id,

        ---------- strings
        option_text_en as option_text,

        ---------- numerics

        ---------- booleans
        is_correct,

        ---------- dates

        ---------- timestamps
        start_utc as starts_at,
        created_at

    from source

)

select * from renamed
