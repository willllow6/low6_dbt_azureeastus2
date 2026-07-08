with

users as (

    select * from {{ ref('STG_ELF_BLAST__USERS') }}

),

players as (

    select * from {{ ref('STG_ELF_BLAST__PLAYERS') }}

),

final as (

    select
        u.user_id,
        p.player_id,
        u.sso_user_id,
        u.username,
        u.email,
        u.tenant,
        p.is_user_active,
        p.is_user_deleted,
        p.has_completed_tutorial,
        p.total_score,
        p.highest_level_completed,
        u.active_streak,
        u.highest_streak,
        u.user_created_at_utc,
        u.user_created_date_utc,
        u.user_created_at_et,
        u.user_created_date_et
    from users as u 
        left join players as p 
            on u.sso_user_id = p.sso_user_id

)

select * from final