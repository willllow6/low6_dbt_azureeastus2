with

entries as (

    select * from {{ ref('oilers_picks__entries') }}

),

calculate_rfm_metrics as (

    select
        user_id,
        max(entry_date_et) as last_entry_date,
        count(*) as entries
    from entries
    group by 
        user_id

),

create_segments as (

    select
        user_id,
        last_entry_date,
        entries,
        datediff('day', last_entry_date ,getdate()) as days_since_last_entry,

        case
            when entries = 1
                then 'Low'
            else 'High'
        end as frequency,

        case 
            when days_since_last_entry <=7 
                then 'High'
            when days_since_last_entry <=21 
                then 'Med'
            when days_since_last_entry > 21 
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