with

entries as (

    select
        user_id,
        region,
        contest_competition,
        entry_date_et   as active_day
    from {{ ref('betway_picks__entries') }}

)

select distinct
    user_id,
    region,
    contest_competition,
    active_day
from entries
