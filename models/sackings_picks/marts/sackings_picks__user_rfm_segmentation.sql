with

--Return all pickem entries
pickem_entries as (

    select *
    from {{ ref('sackings_picks__entries') }}

),

--Return most recent pickem entry and number of entries by user_id and app_name
pickems_users as (

    select
        user_id,
        app_entered_on as app_name, 
        max(entry_date) as most_recent_entry_date,
        count(entry_id) as entries
    from pickem_entries
    group by 
        user_id,
        app_entered_on
    order by 
        user_id,
        app_entered_on

),

--logic/rules to create segments
segmentation as (

    select
        pickems_users.*,
        case
            when entries = 1 then 'Low'
            when entries >1 then 'High'
        end as frequency,
        datediff('day', most_recent_entry_date ,getdate()) as last_played_days_ago,
        case 
            when datediff('day', most_recent_entry_date ,getdate()) <=7 then 'High'
            when datediff('day', most_recent_entry_date ,getdate()) <=21 then 'Med'
            when datediff('day', most_recent_entry_date ,getdate()) > 21 then 'Low'
        end as recency,
        case 
            when frequency = 'Low' and recency = 'High' then 'New Players'
            when frequency ='Low' and recency = 'Med' then 'Recently Tried'
            when frequency ='Low' and recency = 'Low' then 'One Hitter Quitter'
            when frequency ='High' and recency = 'High' then 'Active Players'
            when frequency ='High' and recency = 'Med' then 'At Risk'
            when frequency ='High' and recency = 'Low' then 'Hibernating'
        end as segment_by_app
    from pickems_users

)

select * from segmentation