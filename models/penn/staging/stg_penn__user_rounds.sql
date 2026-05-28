with

source as (

    select *
    from {{ source('penn', 'squads_user_rounds') }}

),

renamed as (

    select

        ---------- ids
        id as user_round_id,
        user_id,
        round_id,
        prize_outcome_id,

        ---------- strings
        user_tier,
        status,
        per_goal_currency,

        ---------- numerics
        per_goal_amount,

        ---------- dates
        cast(cast(created_at as timestamp_ntz) as date) as entry_date,

        ---------- timestamps
        cast(locked_at as timestamp_ntz) as locked_at,
        cast(prize_revealed_at as timestamp_ntz) as prize_revealed_at,
        cast(created_at as timestamp_ntz) as entered_at,
        cast(created_at as timestamp_ntz) as created_at,
        cast(updated_at as timestamp_ntz) as updated_at

    from source

)

select * from renamed
