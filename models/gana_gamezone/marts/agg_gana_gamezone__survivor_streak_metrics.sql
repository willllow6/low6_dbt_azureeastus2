with

streaks as (

    select * from {{ ref('mart_gana_gamezone__survivor_streaks') }}

),

-- One row per streak length showing how many users are currently at that streak
-- and how many users have ever reached it (best_streak >= length)
streak_lengths as (

    select distinct best_streak as streak_length
    from streaks

),

aggregated as (

    select
        sl.streak_length,
        'gana'                                                              as client_id,
        'gana'                                                              as tenant_id,
        'Gana'                                                              as tenant_name,
        'survivor'                                                          as game_type,
        count(case when s.current_streak = sl.streak_length then 1 end)    as active_users,
        count(case when s.best_streak >= sl.streak_length then 1 end)      as users_reached
    from streak_lengths as sl
    left join streaks as s
        on s.best_streak >= sl.streak_length
    group by sl.streak_length

)

select
    client_id,
    tenant_id,
    tenant_name,
    game_type,
    streak_length,
    active_users,
    users_reached
from aggregated
order by streak_length
