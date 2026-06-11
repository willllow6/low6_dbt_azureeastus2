with

survivor_entries as (

    select * from {{ ref('int_gana_gamezone__survivor_entries') }}

)

select
    entry_id,
    score_id,
    user_id,
    client_id,
    tenant_id,
    tenant_name,
    game_type,
    contest_id,
    selection_id,
    selected_country_id,
    is_correct,
    current_streak,
    best_streak,
    selected_at,
    created_at,
    updated_at
from survivor_entries
