select 
    contest_sk,
    contest_name,
    contest_status,
    contest_opens_at,
    contest_opens_at_et,
    contest_starts_at,
    contest_starts_at_et
from {{ ref('stg_saracen_picks__contests') }}
where 1 = 0 -- picks on dev; remove when connected to prod

union all

select
    contest_sk,
    contest_name,
    contest_status,
    contest_opens_at,
    contest_opens_at_et,
    contest_starts_at,
    contest_starts_at_et
from {{ ref('int_saracen_bracket__contests_unioned') }}

