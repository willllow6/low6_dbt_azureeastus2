with

entries as (

    select * 
    from {{ ref('sackings_picks__entries') }}

),

final as (

    select
        contest_name,
        pickem_status as contest_status,
        entry_date,
        pickem_start_date,
        dayname(entry_date) as entry_day,
        hour(entry_time) as entry_hour,
        count(*) as entries,
        sum(case when user_entry_number_app = 1 then 1 else 0 end) as first_entries
    from entries 
    group by 1,2,3,4,5,6

)

select * from final