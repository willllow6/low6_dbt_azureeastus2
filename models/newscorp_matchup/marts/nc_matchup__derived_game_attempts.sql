with

line_attempts as (

    select * from {{ ref('nc_matchup__line_attempts') }}

),

game_attempts as (

    select
        user_id || '-' || game_id as derived_game_attempt_id, 
        user_id,
        sso_user_id,
        service_user_id,
        sport_name,
        game_title,
        game_status,
        game_start_date_aet,
        game_end_date_aet,
        min(line_attempted_at_aet) as game_attempted_at_aet,
        min(line_attempt_date_aet) as game_attempt_date_aet,
        count(*) as line_attempts,
        sum(line_attempt_is_correct) as correct_line_attempts,
        sum(line_attempt_duration_milliseconds) as game_attempt_duration_milliseconds,
        case when correct_line_attempts >= 4 then 'win' else 'fail' end as game_attempt_outcome
    from line_attempts
    group by 1,2,3,4,5,6,7,8,9

),

user_attempt_number as (

    select
        *,
        row_number() over (partition by user_id order by game_attempt_date_aet) as user_attempt_number
    from game_attempts

)

select * from user_attempt_number
