with

game_attempts as (

    select * from {{ ref('stg_nc_matchup__game_attempts') }}

),

users as (

    select * from {{ ref('stg_nc_matchup__users') }}

),

games as (

    select * from {{ ref('stg_nc_matchup__games') }}

),

joined as (

    select
        ga.game_attempt_id,
        ga.user_id,
        u.service_user_id,
        u.sso_user_id,
        u.email,
        u.username,
        u.nickname,
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
        ga.game_attempt_outcome,
        ga.game_attempted_at,
        ga.game_attempt_date,
        ga.game_attempted_at_aet,
        ga.game_attempt_date_aet
    from game_attempts as ga 
        left join users as u 
            on ga.user_id = u.user_id
        left join games as g 
            on ga.game_id = g.game_id

),

final as (

    select
    *,
    rank() over(
        partition by user_id
        order by game_attempted_at_aet
        ) as user_attempt_number
    from joined
)

select * from final
