with

user_rfm_segments as (
    
    select * from {{ ref('bet365_overunder__rfm_segments') }}

),

rfm_segment_stats as (
    
    select
        recency,
        frequency,
        segment,
        count(user_id) as users,
        median(entries) as entries_median,
        median(days_since_last_played) as median_days_since_last_played
    from user_rfm_segments
    group by 1,2,3

)

select * from rfm_segment_stats