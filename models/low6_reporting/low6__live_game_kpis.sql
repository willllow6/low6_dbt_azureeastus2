with

bet365_overunder as (

    select
        '1195' as app_id,
        count(*) as entries,
        count(case when cast(created_at as date) = current_date() - 1 then id else null end) as yesterday_entries,
        count(case when created_at >= current_date() - 8 and created_at < current_date() then id else null end) as last_7_days_entries,
        count(case when created_at >= current_date() - 29 and created_at < current_date() then id else null end) as last_28_days_entries,
        count(distinct user_id) as entrants,
        count(distinct case when cast(created_at as date) = current_date() - 1 then user_id else null end) as yesterday_entrants,
        count(distinct case when created_at >= current_date() - 8 and created_at < current_date() then user_id else null end) as last_7_days_entrants,
        count(distinct case when created_at >= current_date() - 29 and created_at < current_date() then user_id else null end) as last_28_days_entrants,
        count(distinct cast(created_at as date)) as contests,
        max(cast(created_at as date)) as last_entry_date
    from  {{ source('bet365_overunder', 'entries') }}
    group by 1

)


select * from bet365_overunder