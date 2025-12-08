WITH 

monthly_user_activity AS (

    SELECT
        user_id,
        region,
        DATE_TRUNC('month', date_day) AS month_start,
        MAX(is_active) AS was_active_this_month,
        max(is_registration) as was_registered_this_month,
        SUM(entries_count) AS entries_this_month
    FROM {{ ref('betway_picks__daily_user_activity') }}
    GROUP BY 1,2,3
    
),

previous_month_activity as (

    select
        *,
        LAG(was_active_this_month) OVER (PARTITION BY user_id ORDER BY month_start) AS prev_active
    from monthly_user_activity

),

monthly_retained as (

    SELECT
        *,
        CASE 
            WHEN was_active_this_month = 1 AND prev_active = 1 THEN 1
            ELSE 0
        END AS was_retained_this_month
    FROM previous_month_activity

)

SELECT * from monthly_retained