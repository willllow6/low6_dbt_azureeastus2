with

source as (

    select * from {{ source('newscorp_matchup','users') }} 

),

renamed as (

    select
        userid as user_id,
        ssouserid as sso_user_id,
        serviceuserid as service_user_id,
        username,
        nickname,
        email,
        createdat as user_created_at,
        cast(createdat as date) as user_created_date,
        convert_timezone('UTC','Australia/Sydney',createdat) as user_created_at_aet,
        cast(user_created_at_aet as date) as user_created_date_aet,
        updatedat as user_updated_at
    from source

) 

select * from renamed