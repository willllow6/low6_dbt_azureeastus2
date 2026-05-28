with

source as (

    select *
    from {{ source('penn', 'squads_awards') }}

),

renamed as (

    select

        ---------- ids
        id as award_id,
        user_round_id as entry_id,
        penn_user_id,
        goal_id,
        goal_external_id,
        player_external_id,
        event_external_id,
        outcome_id,
        promotion_id,
        penn_session_id,

        ---------- strings
        platform,
        status as award_status,
        currency,

        ---------- numerics
        amount as prize_amount,
        attempts,
        last_response_status,

        ---------- timestamps
        cast(next_attempt_at as timestamp_ntz) as next_attempt_at,
        cast(last_attempt_at as timestamp_ntz) as last_attempt_at,
        cast(delivered_at as timestamp_ntz) as delivered_at,
        cast(created_at as timestamp_ntz) as created_at,
        cast(updated_at as timestamp_ntz) as updated_at

    from source

)

select * from renamed
