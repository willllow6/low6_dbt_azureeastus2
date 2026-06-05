with

selections as (

    select *
    from {{ ref('fct_opap_spintowin__selections') }}

),

users as (

    select
        user_id,
        sso_user_id,
        user_state
    from {{ ref('dim_opap_spintowin__users') }}

)

select
    selections.selection_id,
    selections.entry_id,
    selections.user_id,
    users.sso_user_id,
    users.user_state,
    selections.game_type,
    selections.event_id,
    selections.market_id,
    selections.outcome_id,
    selections.event_name,
    selections.market_name,
    selections.selection_sequence,
    selections.selection_value,
    selections.selection_status,
    selections.created_at,
    selections.updated_at
from selections
inner join users
    on selections.user_id = users.user_id
