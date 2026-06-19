select
    s.selection_id,
    s.entry_id,
    e.user_id,
    e.sso_user_id,
    s.match_id,
    s.selected_team_id,
    t.team_name as selected_team_name,
    s.created_date,
    s.created_at,
    s.updated_at
from {{ ref('int_saracen_bracket__selections_unioned') }} as s
left join {{ ref('int_saracen_bracket__teams_unioned') }} as t
    on s.selected_team_id = t.team_id
left join {{ ref('int_saracen_bracket__entries_unioned') }} as e
    on s.entry_id = e.entry_id