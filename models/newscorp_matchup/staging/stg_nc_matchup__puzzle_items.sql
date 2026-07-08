with

source as (

    select * from {{ source('newscorp_matchup','puzzle_items') }} 

),

renamed as (

    select
        id as puzzle_item_id,
        categoryid as category_id,
        name as puzzle_item_name,
        languagecode as sport_code,
        createdat as item_created_at,
        updatedat as item_updated_at
    from source

) 

select * from renamed