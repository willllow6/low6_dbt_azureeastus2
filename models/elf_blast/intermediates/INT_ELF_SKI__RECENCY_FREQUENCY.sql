with

entries as (

    select * from {{ ref('INT_ELF_SKI__SESSIONS') }}

),


user_sessions as (

    select
        user_id,
        max(session_date_et) as most_recent_session_date_et,
        count(session_id) as sessions_count
    from entries
    group by 
        user_id
    order by 
        user_id

),

--logic/rules to create segments
segmentation as (

    select
        u.*,
        case
            when sessions_count = 1 
                then 'Low'
            when sessions_count > 1 
                then 'High'
        end as frequency,
        datediff('day', most_recent_session_date_et ,getdate()) as last_played_days_ago,
        case 
            when datediff('day', most_recent_session_date_et ,getdate()) <=7 
                then 'High'
            when datediff('day', most_recent_session_date_et ,getdate()) <=21 
                then 'Med'
            when datediff('day', most_recent_session_date_et ,getdate()) > 21 
                then 'Low'
        end as recency,
        case 
            when frequency = 'Low' and recency = 'High' 
                then 'New Users'
            when frequency ='Low' and recency = 'Med' 
                then 'Recently Tried'
            when frequency ='Low' and recency = 'Low' 
                then 'One Hitter Quitter'
            when frequency ='High' and recency = 'High' 
                then 'Active Users'
            when frequency ='High' and recency = 'Med' 
                then 'At Risk'
            when frequency ='High' and recency = 'Low' 
                then 'Hibernating'
        end as segment
    from user_sessions as u

)

select * from segmentation