with

source as (

    select * 
    from {{ source('bet365_overunder','entries') }}

),

renamed as (

    select

        id as entry_id,
        user_id,
        
        cast(convert_timezone('UTC','America/New_York',to_timestamp_ntz(locked_at)) as date) as contest_date_et,
        total_picks as entered_picks,
        scored_picks,
        correct_picks,
        potential_prize as potential_prize_amount,
        won_amount as prize_amount,

        case when prize_amount > 0 then true else false end as is_winner,

        hour(to_timestamp_ntz(created_at)) as entry_hour,
        hour(convert_timezone('UTC','America/New_York',to_timestamp_ntz(created_at))) as entry_hour_et,

        cast(to_timestamp_ntz(created_at) as date) as entry_date,
        cast(convert_timezone('UTC','America/New_York',to_timestamp_ntz(created_at)) as date) as entry_date_et,

        to_timestamp_ntz(created_at) as entered_at,
        convert_timezone('UTC','America/New_York',to_timestamp_ntz(created_at)) as entered_at_et,
        to_timestamp_ntz(locked_at) as locked_at,
        convert_timezone('UTC','America/New_York',to_timestamp_ntz(locked_at)) as locked_at_et,
        to_timestamp_ntz(settled_at) as settled_at,
        convert_timezone('UTC','America/New_York',to_timestamp_ntz(settled_at)) as settled_at_et
    
    from source

)

select * from renamed