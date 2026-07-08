with

entries as (
  
     select * from {{ ref('nc_matchup__derived_game_attempts') }} 

),

final as (

    select
        sport_name,
        game_title,
        game_status,
        game_start_date_aet,
        game_attempt_date_aet,
        dayname(game_attempt_date_aet) as entry_day,
        hour(game_attempted_at_aet) as entry_hour,
        game_attempt_outcome,
        case when correct_line_attempts > 4 then 4 else correct_line_attempts end as correct_line_attempts,
        count(*) as game_attempts,
        sum(case when user_attempt_number = 1 then 1 else 0 end) as users,
        sum(case when user_attempt_number = 2 then 1 else 0 end) as active_users,
        sum(case when user_attempt_number >= 2 then 1 else 0 end) as active_user_attempts,
        sum(line_attempts) as line_attempts
    from entries
    group by 1,2,3,4,5,6,7,8,9

)

select * from final