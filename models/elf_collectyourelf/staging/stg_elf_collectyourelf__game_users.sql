with

source as (

    select *
    from {{ source('collectyourelf','game_user') }}

),

renamed as (

    select

        ----------  ids
        user_id,
        game_id,

        ---------- strings

        ---------- numerics
        total_coins,
    
        ---------- booleans

        ---------- dates
        cast(created_at as date) as registration_date,
        cast(convert_timezone('UTC','America/New_York',created_at) as date) as registration_date_et,

        ---------- timestamps
        created_at,
        updated_at

    from source

)

select * from renamed