select
    league_name,
    joined_date_et,
    count(*) as league_members
from {{ ref('mart_saracen_picks__friends_league_members') }}
group by 1,2