with

players as (

    select * from {{ source('ELF_BLAST', 'LEVEL_PLAYER') }} 
    -- where _fivetran_deleted = False 

),

final as (

    select
        id as player_id,
        player_ref_id as sso_user_id,
        email,
        player_name as user_name,
        active as is_user_active,
        deleted as is_user_deleted,
        istutorialcomplete as has_completed_tutorial,
        score as total_score,
        level_no as highest_level_completed,
        created_at as player_created_at_utc,
        cast(created_at as date) as player_created_date_utc,
        convert_timezone('UTC','America/New_York',player_created_at_utc) as player_created_at_et,
        cast(player_created_at_et as date) as player_created_date_et
    from players

)

select * from final