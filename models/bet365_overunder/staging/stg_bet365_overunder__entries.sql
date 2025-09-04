with

source as (

    select * 
    from {{ source('bet365_overunder','entries') }}

),

renamed as (

    select

        id as entry_id,
        user_id,
        
        total_picks as entered_picks,
        scored_picks,
        correct_picks,
        potential_prize as potential_prize_amount,
        won_amount as prize_amount,

        case when prize_amount > 0 then true else false end as is_winner,

        hour(created_at) as entry_hour,

        cast(created_at as date) as entry_date,

        created_at as entered_at,
        locked_at,
        settled_at
    
    from source

)

select * from renamed