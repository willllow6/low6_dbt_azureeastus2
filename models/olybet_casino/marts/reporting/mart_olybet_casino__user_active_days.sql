with

entries as (

    select
        user_id,
        entry_date_et   as active_day,
        client_id,
        tenant_id,
        tenant_name,
        game_type
    from {{ ref('fct_olybet_casino__entries') }}

)

select distinct
    user_id,
    active_day,
    client_id,
    tenant_id,
    tenant_name,
    game_type
from entries
