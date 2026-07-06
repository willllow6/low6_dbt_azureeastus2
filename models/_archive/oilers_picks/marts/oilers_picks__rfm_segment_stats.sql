with

user_rfm_segments as (
    
    select * from {{ ref('oilers_picks__user_rfm_segments') }}

),

rfm_segment_stats as (
    
    select
        recency,
        frequency,
        segment,
        count(user_id) as users_count,
        median(entries) as entries_median,
        median(days_since_last_entry) as days_since_last_entry_median
    from user_rfm_segments
    group by 1,2,3

)

select * from rfm_segment_stats