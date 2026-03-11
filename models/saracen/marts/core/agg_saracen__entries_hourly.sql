with

user_entries as (
  
     select * from {{ ref('mart_saracen__entries') }} 

),

entry_stats as (

    select
        game_type,
        contest_name,
        contest_status,
        contest_starts_at_et,
        entry_date_et,
        entry_hour_et,
        count(*) as entries,
        sum(case when user_entry_number = 1 then 1 else 0 end) as first_entries,
        sum(case when user_game_entry_number = 1 then 1 else 0 end) as first_game_entries
    from user_entries
    group by 1,2,3,4,5,6

)

select * from entry_stats
