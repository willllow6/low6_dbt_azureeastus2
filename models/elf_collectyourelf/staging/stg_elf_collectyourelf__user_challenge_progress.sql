with

source as (

    select *
    from {{ source('collectyourelf','user_challenge_progress') }}

),

renamed as (

    select

        ----------  ids
        user_id,
        challenge_id,

        ---------- strings

        ---------- numerics
    
        ---------- booleans
        is_completed,

        ---------- dates
        cast(completed_at as date) as challenge_completion_date,
        cast(convert_timezone('UTC','America/New_York',completed_at) as date) as challenge_completion_date_et,

        ---------- timestamps
        completed_at,
        created_at,
        updated_at

    from source

)

select * from renamed