with

source as (

    select *
    from {{ source('bet99_picks', 'questions') }}

),

renamed as (

    select
        ----------  ids
        id as question_id,
        pickem_id as contest_id,

        ---------- strings
        question_text_en as question_text,
        question_type as question_type,

        ---------- numerics
        correct_value,
        points,

        ---------- booleans

        ---------- dates
        cast(created_at as date) as created_date,

        ---------- timestamps
        start_utc as starts_at,
        created_at as created_at

    from source

)

select * from renamed
