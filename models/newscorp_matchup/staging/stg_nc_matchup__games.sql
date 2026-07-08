with

source as (

    select * from {{ source('newscorp_matchup','games') }} 

),

renamed as (

    select
        id as game_id,
        categoryids as category_ids,
        title as game_title,
        status as game_status,
        languagecode as sport_code,
        starttime as game_starts_at,
        cast(starttime as date) as game_start_date,
        convert_timezone('UTC','Australia/Sydney',starttime) as game_starts_at_aet,
        cast(game_starts_at_aet as date) as game_start_date_aet,
        endtime as game_ends_at,
        cast(endtime as date) as game_end_date,
        convert_timezone('UTC','Australia/Sydney',endtime) as game_ends_at_aet,
        cast(game_ends_at_aet as date) as game_end_date_aet,
        createdat as game_created_at,
        updatedat as game_updated_at
    from source

) 

select * from renamed