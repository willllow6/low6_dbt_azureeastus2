with

questions as (

    select *
    from {{ ref('stg_du_trivia__questions') }}

)

select
    question_id,
    contest_id,

    client_id,
    tenant_id,
    tenant_name,
    game_type,

    question_title,
    question_title_ar,
    question_image,
    question_type,

    question_duration,
    question_points,

    created_at,
    updated_at

from questions
