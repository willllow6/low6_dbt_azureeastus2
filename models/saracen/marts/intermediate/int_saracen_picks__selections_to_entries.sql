select
    'PKM_' || s.user_id || '-' || s.contest_id as entry_sk,
    s.sso_user_id,
    s.user_id,
    s.contest_id,
    s.contest_sk,
    'pickem' as game_type,
    sum(s.points) as points,
    max(s.tiebreaker_prediction) as tiebreaker_prediction,
    max(q.correct_value) as tiebreaker_outcome,
    round(abs(max(s.tiebreaker_prediction) - max(q.correct_value))) as tiebreaker_error,
    min(s.created_date) as entry_date,
    day(min(s.created_date)) as entry_day,
    min(s.created_at) as entered_at,
    min(s.created_date_et) as entry_date_et,
    day(min(s.created_date_et)) as entry_day_et,
    min(s.created_at_et) as entered_at_et
from {{ ref('stg_saracen_picks__user_selections') }} as s
left join {{ ref('stg_saracen_picks__questions') }} as q
    on s.contest_id = q.contest_id
    and question_type = 'tiebreaker'
group by 1,2,3,4,5