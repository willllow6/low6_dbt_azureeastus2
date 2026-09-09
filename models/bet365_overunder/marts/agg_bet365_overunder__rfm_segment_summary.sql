with

user_rfm_segments as (

    select *
    from {{ ref('agg_bet365_overunder__user_rfm_segments') }}

),

rfm_segment_summary_by_country as (

    select
        client_id,
        tenant_id,
        country,
        recency,
        frequency,
        segment,
        count(user_id) as users,
        median(entries) as entries_median,
        median(days_since_last_played) as median_days_since_last_played
    from user_rfm_segments
    group by 1,2,3,4,5,6

),

rfm_segment_summary_all_countries as (

    select
        client_id,
        tenant_id,
        'All' as country,
        recency,
        frequency,
        segment,
        count(user_id) as users,
        median(entries) as entries_median,
        median(days_since_last_played) as median_days_since_last_played
    from user_rfm_segments
    group by 1,2,4,5,6

),

unioned as (

    select * from rfm_segment_summary_by_country
    union all
    select * from rfm_segment_summary_all_countries

)

select * from unioned
