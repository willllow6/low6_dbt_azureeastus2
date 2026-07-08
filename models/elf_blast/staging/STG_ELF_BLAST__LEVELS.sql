with

levels as (

    select * from {{ source('ELF_BLAST', 'GAME_LEVEL') }} 
    -- where _fivetran_deleted = False 

),

final as (

    select
        id as level_id,
        active as is_level_active,
        deleted as is_level_deleted,
        level_no as level_number,
        title as level_name,
        description as level_description,
        score_count as level_completion_score,
        move_count as moves_allowed,
        obstacle_json,
        created_at as level_created_at_utc,
        cast(created_at as date) as level_created_date_utc,
        convert_timezone('UTC','America/New_York',level_created_at_utc) as level_created_at_et,
        cast(level_created_at_et as date) as level_created_date_et
    from levels

)

select * from final