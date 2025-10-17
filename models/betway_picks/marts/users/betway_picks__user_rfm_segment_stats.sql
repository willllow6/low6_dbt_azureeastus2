with

user_rfm_segments as (
    
    select * from {{ ref('betway_picks__user_rfm_segments') }}

),

rfm_segment_stats as (
    
    select
        recency,
        frequency,
        segment,
        count(user_id) as users,
        median(user_entries_count) as entries_median,
        median(days_since_last_played) as days_since_last_played_median
    from user_rfm_segments
    group by 1,2,3

)

select * from rfm_segment_stats