with

selections as (

    select *
    from {{ ref('int_opap_spintowin__selections') }}

)

select
    selection_id,
    entry_id,
    user_id,
    contest_id,
    client_id,
    tenant_id,
    game_type,
    event_id,
    market_id,
    outcome_id,
    event_name,
    market_name,
    selection_sequence,
    selection_value,
    selection_status,
    created_at,
    cast(convert_timezone('UTC', '{{ var("opap_spintowin_local_timezone") }}', created_at) as date) as created_date_et,
    convert_timezone('UTC', '{{ var("opap_spintowin_local_timezone") }}', created_at)::timestamp_ntz as created_at_et,
    updated_at
from selections
