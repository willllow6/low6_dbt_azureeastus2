with

entries as (
  
     select * from {{ ref('nc_matchup__game_attempts') }} 

),

final as (

    select
        game_title,
        game_status,
        game_start_date_aet,
        game_attempt_date_aet,
        dayname(game_attempt_date_aet) as entry_day,
        hour(game_attempted_at_aet) as entry_hour,
        game_attempt_outcome,
        count(*) as attempts,
        sum(case when user_attempt_number = 1 then 1 else 0 end) as users
    from entries
    group by 1,2,3,4,5,6,7

)

select * from final