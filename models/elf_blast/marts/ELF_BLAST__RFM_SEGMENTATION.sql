with

user_segmentation as (
    
    select * from {{ ref('INT_ELF_BLAST__RECENCY_FREQUENCY') }}

),

--summarise the user segmentation to an app level
summary as (
    select
        recency,
        frequency,
        segment,
        count(user_id) as users_count,
        median(sessions_count) as sessions_median,
        median(last_played_days_ago) as last_played_median_days
    from user_segmentation
    group by 1,2,3
    order by 1,2,3
)

select * from summary