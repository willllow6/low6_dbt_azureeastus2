with

user_entries as (
  
     select * from {{ ref('bet99_picks__entries') }} 

),

entry_stats as (

    select
        
        region,
        contest_title,
        contest_status,
        contest_start_date,
        
        user_entry_type,
        entry_date,
        entry_day,
        entry_hour,
        entry_date_et,
        entry_day_et,
        entry_hour_et,

        count(*) as entries,
        sum(case when user_entry_number = 1 then 1 else 0 end) as first_entries
    from user_entries
    group by 1,2,3,4,5,6,7,8,9,10,11

)

select * from entry_stats
