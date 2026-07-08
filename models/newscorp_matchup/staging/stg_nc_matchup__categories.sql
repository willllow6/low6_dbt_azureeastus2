with

source as (

    select * from {{ source('newscorp_matchup','categories') }} 

),

renamed as (

    select
        id as category_id,
        name as category_name,
        difficulty as category_difficulty_level,
        languagecode as sport_code,
        createdat as created_at,
        updatedat as updated_at
    from source

) 

select * from renamed