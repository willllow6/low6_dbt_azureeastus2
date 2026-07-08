with

source as (

    select * from {{ source('newscorp_matchup','past_games') }} 

),

renamed as (

    select
        id as game_attempt_id,
        userid as user_id,
        gameid as game_id,
        status as game_attempt_outcome,
        createdat as game_attempted_at,
        cast(createdat as date) as game_attempt_date,
        convert_timezone('UTC','Australia/Sydney',createdat) as game_attempted_at_aet,
        cast(game_attempted_at_aet as date) as game_attempt_date_aet,
        updatedat as game_attempt_updated_at
    from source

) 

select * from renamed