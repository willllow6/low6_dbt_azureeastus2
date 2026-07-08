with

source as (

    select * from {{ source('newscorp_matchup','user_attempts') }} 

),

renamed as (

    select
        id as line_attempt_id,
        userid as user_id,
        gameid as game_id,
        attemptedgroup as line_attempt_selections,
        duration as line_attempt_duration_milliseconds,
        createdat as line_attempted_at,
        cast(createdat as date) as line_attempt_date,
        convert_timezone('UTC','Australia/Sydney',createdat) as line_attempted_at_aet,
        cast(line_attempted_at_aet as date) as line_attempt_date_aet,
        updatedat as line_attempt_updated_at
    from source

) 

select * from renamed