with

selections as (

    select *
    from {{ ref('stg_du_trivia__selections') }}

)

select
    selection_id,
    entry_id,
    user_id,
    contest_id,
    question_id,
    option_id,

    client_id,
    tenant_id,
    tenant_name,
    game_type,

    selection_points,
    elapsed_seconds,
    client_provided_points,
    client_provided_elapsed_seconds,

    is_correct,

    started_at,
    selected_at,
    convert_timezone('UTC', '{{ var("du_trivia_local_timezone") }}', selected_at) as selected_at_gst,
    updated_at

from selections
