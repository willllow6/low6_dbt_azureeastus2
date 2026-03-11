select
    lm.friends_league_member_id,
    lm.user_id,
    lm.league_id,
    u.sso_user_id,
    l.league_name,
    u.username,
    u.email,
    lm.joined_date,
    lm.joined_date_et,
    lm.joined_at,
    lm.joined_at_et
from {{ ref('fct_saracen_picks__friends_league_members') }} as lm
left join {{ ref('dim_saracen_picks__leagues') }} as l
    on lm.league_id = l.league_id
left join {{ ref('dim_saracen_picks__users') }} as u 
    on lm.user_id = u.user_id