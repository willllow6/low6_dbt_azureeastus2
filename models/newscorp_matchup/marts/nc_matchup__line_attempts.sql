with

line_attempts as (

    select * from {{ ref('stg_nc_matchup__line_attempts') }}

),

users as (

    select * from {{ ref('stg_nc_matchup__users') }}

),

games as (

    select * from {{ ref('stg_nc_matchup__games') }}

),

sports as (

    select * from {{ ref('stg_nc_matchup__sports') }}

),

line_attempt_selections as (

    select * from {{ ref('nc_matchup__line_attempt_selections') }}

),

line_attempt_outcomes as (

    select
        line_attempt_id,
        case 
            when count(distinct category_id) = 1 
                then 1 
            else 0 
        end as line_attempt_is_correct
    from line_attempt_selections
    group by 1

),

final as (

    select
        la.line_attempt_id,
        la.user_id,
        u.service_user_id,
        u.sso_user_id,
        u.email,
        u.username,
        u.nickname,
        case 
            when g.sport_code = 'en'
                then 'Cricket'
            else s.sport_name
        end as sport_name,
        g.game_id,
        g.game_title,
        g.game_status,
        g.game_starts_at,
        g.game_start_date,
        g.game_starts_at_aet,
        g.game_start_date_aet,
        g.game_ends_at,
        g.game_end_date,
        g.game_ends_at_aet,
        g.game_end_date_aet,
        la.line_attempt_selections,
        la.line_attempt_duration_milliseconds,
        lao.line_attempt_is_correct,
        la.line_attempted_at,
        la.line_attempt_date,
        la.line_attempted_at_aet,
        la.line_attempt_date_aet
    from line_attempts as la 
        left join users as u 
            on la.user_id = u.user_id
        left join games as g 
            on la.game_id = g.game_id
        left join line_attempt_outcomes as lao
            on la.line_attempt_id = lao.line_attempt_id
        left join sports as s 
            on g.sport_code = s.sport_code

)

select * from final
