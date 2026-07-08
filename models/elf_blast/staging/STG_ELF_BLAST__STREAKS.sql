with

streaks as (

    select * from {{ source('ELF_BLAST', 'LOGIN_STREAK_HISTORY') }} 
    -- where _fivetran_deleted = False 

),

final as (

    select
        logid as streak_id,
        userid as user_id,
        streak as streak_reached,
        createdat as streak_created_at_utc,
        cast(createdat as date) as streak_created_date_utc,
        convert_timezone('UTC','America/New_York',streak_created_at_utc) as streak_created_at_et,
        cast(streak_created_at_et as date) as streak_created_date_et
    from streaks

)

select * from final