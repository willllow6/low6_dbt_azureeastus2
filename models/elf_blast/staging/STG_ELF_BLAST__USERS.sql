with

users as (

    select * from {{ source('ELF_BLAST', 'USERS') }} 
    -- where _fivetran_deleted = False 

),

final as (

    select
        userid as user_id,
        ServiceUserId as sso_user_id,
        username,
        email,
        tenant,
        higheststreak as highest_streak,
        activestreak as active_streak,
        createdat as user_created_at_utc,
        cast(createdat as date) as user_created_date_utc,
        convert_timezone('UTC','America/New_York',user_created_at_utc) as user_created_at_et,
        cast(user_created_at_et as date) as user_created_date_et
    from users

)

select * from final