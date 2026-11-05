with

options as (

    select *
    from {{ ref('stg_du_trivia__options') }}

)

select
    option_id,
    question_id,

    client_id,
    tenant_id,
    tenant_name,
    game_type,

    option_title,
    option_title_ar,

    is_correct,

    created_at,
    updated_at

from options
