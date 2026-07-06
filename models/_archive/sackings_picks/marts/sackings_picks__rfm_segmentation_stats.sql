with

user_segmentation as (
    
    select*
    from {{ ref('sackings_picks__user_rfm_segmentation') }}

),

--summarise the user segmentation to an app level
summary as (

    select
        app_name,
        recency,
        frequency,
        segment_by_app,
        count(user_id) as users,
        median(entries) as entries_median,
        median(last_played_days_ago) as last_payed_median_days
    from user_segmentation
    group by 1,2,3,4
    order by 1,2,3,4

)

select * from summary