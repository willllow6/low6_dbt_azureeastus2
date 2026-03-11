select
    m.match_id,
    m.conference_id,
    m.round_id,
    m.home_team_id,
    m.away_team_id,
    m.winner_team_id,
    m.contest_name,
    c.conference_name,
    r.round_name,
    r.round_points,
    t1.team_name as home_team_name,
    t2.team_name as away_team_name,
    t3.team_name as winning_team_name,
    m.match_slot,
    m.is_scored,
    m.match_starts_at
from {{ ref('stg_saracen_bracket__matches')}} as m 
left join {{ ref('stg_saracen_bracket__conferences')}} as c 
    on m.conference_id = c.conference_id
left join {{ ref('stg_saracen_bracket__rounds')}} as r 
    on m.round_id = r.round_id 
left join {{ ref('stg_saracen_bracket__teams')}} as t1
    on m.home_team_id = t1.team_id
left join {{ ref('stg_saracen_bracket__teams')}} as t2
    on m.away_team_id = t2.team_id
left join {{ ref('stg_saracen_bracket__teams')}} as t3
    on m.winner_team_id = t3.team_id
