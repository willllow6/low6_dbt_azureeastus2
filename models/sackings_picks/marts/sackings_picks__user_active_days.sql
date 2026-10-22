with

entries as (

    select
        user_id,
        app_entered_on,
        entry_date      as active_day
    from {{ ref('sackings_picks__entries') }}

)

select distinct
    user_id,
    app_entered_on,
    active_day
from entries
