with

source as (

    select *
    from {{ source('bet99_picks', 'user_selections') }}

),

renamed as (

    select

        ----------  ids
        id as selection_id,
        user_id,
        pickem_id as contest_id,
        question_id,
        option_id,

        ---------- strings
        -- bonus_selection_value,

        ---------- numerics
        try_to_number(substr(custom_value, 0, 5)) as tiebreaker_prediction,
        points_earned as points,

        ---------- booleans

        ---------- dates
        cast(created_at as date) as created_date,
        cast(convert_timezone('UTC','America/New_York',created_at) as date) as created_date_et,

        ---------- timestamps
        created_at,
        updated_at,
        convert_timezone('UTC','America/New_York',created_at) as created_at_et

    from source

)

select * from renamed
