select
    contest_name,
    contest_start_date_et,
    contest_status,
    contest_sport,
    contest_league,
    athlete_position,
    athlete_name,
    count(*) as selections 
from {{ ref('fanstake_rivals__lineup_selections') }}
group by 1,2,3,4,5,6,7