with

level_scores as (

    select * from {{ source('ELF_BLAST', 'PLAYER_LEVEL_SCORE') }} 
    -- where _fivetran_deleted = False 

),

final as (

    select
        id as level_score_id,
        fk_player_id as player_id,
        fk_level_id as level_id,
        player_ref_id as sso_user_id,
        player_name as username,
        score,
        created_at as level_score_created_at_utc,
        cast(created_at as date) as level_score_created_date_utc,
        convert_timezone('UTC','America/New_York', level_score_created_at_utc) as level_score_created_at_et,
        cast(level_score_created_at_et as date) as level_score_created_date_et
    from level_scores

)

select * from final