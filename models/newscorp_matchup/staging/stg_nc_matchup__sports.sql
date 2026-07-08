with

source as (

    select * from {{ source('newscorp_matchup','languages') }} 

),

renamed as (

    select
        id as sport_id,
        code as sport_code,
        name as sport_name,
        createdat as created_at,
        updatedat as updated_at
    from source

) 

select * from renamed