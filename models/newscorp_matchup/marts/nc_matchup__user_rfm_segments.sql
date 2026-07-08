with

--Return all pickem entries
attempts as (

    select * from {{ ref('nc_matchup__game_attempts') }}
    
),

--Return most recent pickem attempt and number of entries by user_id and app_name
user_attempts_summary as (

    select
        user_id,
        sso_user_id,
        max(game_attempt_date_aet) as most_recent_attempt_date,
        count(game_attempt_id) as attempts_count
    from attempts
    group by 
        user_id,
        sso_user_id

),

--logic/rules to create segments
segmentation as (

    select
        *,
        case
            when attempts_count = 1 
                then 'Low'
            else 'High'
        end as frequency,
        datediff('day', most_recent_attempt_date ,getdate()) as last_played_days_ago,
        case 
            when datediff('day', most_recent_attempt_date ,getdate()) <=7 then 'High'
            when datediff('day', most_recent_attempt_date ,getdate()) <=21 then 'Med'
            when datediff('day', most_recent_attempt_date ,getdate()) > 21 then 'Low'
        end as recency,
        case 
            when frequency = 'Low' and recency = 'High' then 'New Users'
            when frequency ='Low' and recency = 'Med' then 'Recently Tried'
            when frequency ='Low' and recency = 'Low' then 'One Hitter Quitter'
            when frequency ='High' and recency = 'High' then 'Active Users'
            when frequency ='High' and recency = 'Med' then 'At Risk'
            when frequency ='High' and recency = 'Low' then 'Hibernating'
        end as segment
    from user_attempts_summary

)

select * from segmentation