
select
    recency,
    frequency,
    segment,
    count(sso_user_id) as users,
    median(user_entries_count) as entries_median,
    median(days_since_last_played) as days_since_last_played_median
from {{ ref('dim_saracen__users') }}
where segment is not null
group by 1,2,3

