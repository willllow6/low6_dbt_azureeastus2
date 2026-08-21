with

entries as (

    select
        user_id,
        region,
        entry_date_et   as active_day
    from {{ ref('betway_picks__entries') }}

)

select distinct
    user_id,
    region,
    active_day
from entries
