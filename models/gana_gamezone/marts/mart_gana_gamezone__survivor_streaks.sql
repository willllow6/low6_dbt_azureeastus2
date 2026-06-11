with

survivor_scores as (

    select * from {{ ref('stg_gana_gamezone__survivor_scores') }}

),

users as (

    select * from {{ ref('dim_gana_gamezone__users') }}

),

latest_per_user as (

    select
        user_id,
        current_streak,
        best_streak,
        created_at          as last_score_at
    from survivor_scores
    qualify row_number() over (partition by user_id order by created_at desc) = 1

)

select
    u.user_id,
    u.email,
    u.full_name,
    l.current_streak,
    l.best_streak,
    l.last_score_at
from latest_per_user as l
left join users as u
    on l.user_id = u.user_id
