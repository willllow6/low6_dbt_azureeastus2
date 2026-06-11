with

predictor_selections as (

    select * from {{ ref('int_gana_gamezone__predictor_selections') }}

)

select
    selection_id,
    entry_id,
    user_id,
    client_id,
    tenant_id,
    tenant_name,
    game_type,
    contest_id,
    group_id,
    predicted_position,
    selected_country_id,
    is_predicted_to_progress,
    selected_at
from predictor_selections
