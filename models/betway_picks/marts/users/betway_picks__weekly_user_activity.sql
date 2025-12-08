WITH 

weekly_user_activity AS (

    SELECT
        user_id,
        region,
        DATE_TRUNC('week', date_day) AS week_start,
        max(is_active) AS was_active_this_week,
        max(is_registration) as was_registered_this_week,
        sum(entries_count) AS entries_this_week
    FROM {{ ref('betway_picks__daily_user_activity') }}
    GROUP BY 1,2,3

),

previous_week_activity as (

    select
        *,
        LAG(was_active_this_week) OVER (PARTITION BY user_id ORDER BY week_start) AS prev_active
    from weekly_user_activity

),

weekly_retained as (

    SELECT
        *,
        CASE 
            WHEN was_active_this_week = 1 AND prev_active = 1 THEN 1
            ELSE 0
        END AS was_retained_this_week
    FROM previous_week_activity

)

SELECT * from weekly_retained