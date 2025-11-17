with

entries as (
  
     select * from {{ ref('oilers_picks__entries') }} 

),

entry_stats as (

    select
        has_consented_marketing,
        has_placed_sports_bet,
        
        pickem_title,
        pickem_status,
        pickem_start_date_et,
        
        user_entry_type,
        entry_date_et,
        entry_day_et,
        entry_hour_et,

        count(*) as entries,
        sum(case when user_entry_number = 1 then 1 else 0 end) as first_entries
    from entries
    group by 1,2,3,4,5,6,7,8,9

)

select * from entry_stats
