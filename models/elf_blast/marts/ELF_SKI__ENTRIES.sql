with

entries as (

    select * from {{ ref('INT_ELF_SKI__ENTRIES') }}

),

final as (

    select
        tenant,
        entry_date_utc,
        entry_date_et,
        entry_hour_et,
        case 
            when user_entry_number = 1
                then 'First Entry'
            else 'Subsequent Entry'
        end as entry_type,
        count(*) as entries,
        sum(case when user_entry_number = 1 then 1 else 0 end) as users,
        sum(entry_elapsed_time_seconds) as total_elapsed_time,
        sum(case when entry_elapsed_time_seconds is not null then 1 else 0 end) as timed_entries
    from entries
    group by 1,2,3,4,5

)

select * from final