with

user_entries as (

    select * from {{ ref('betway_picks__entries') }}

),

calculate_rfm_metrics as (

    select
        user_id,
        region,
        max(entry_date_et) as last_entered_date,
        count(*) as user_entries_count
    from user_entries
    group by 1,2

),

create_segments as (

    select
        user_id,
        region,
        last_entered_date,
        user_entries_count,
        datediff('day', last_entered_date ,getdate()) as days_since_last_played,

        case
            when user_entries_count = 1
                then 'Low'
            else 'High'
        end as frequency,

        case 
            when days_since_last_played <=7 
                then 'High'
            when days_since_last_played <=21 
                then 'Med'
            when days_since_last_played > 21 
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

    from calculate_rfm_metrics

)

select * from create_segments