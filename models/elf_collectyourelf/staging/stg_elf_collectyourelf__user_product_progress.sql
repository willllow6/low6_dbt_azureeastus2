with

source as (

    select *
    from {{ source('collectyourelf','user_product_progress') }}

),

renamed as (

    select

        ----------  ids
        user_id,
        challenge_id,
        product_id,

        ---------- strings

        ---------- numerics
    
        ---------- booleans
        is_found,

        ---------- dates
        cast(found_at as date) as product_found_date,
        cast(convert_timezone('UTC','America/New_York',found_at) as date) as product_found_date_et,

        ---------- timestamps
        found_at,
        created_at,
        updated_at

    from source

)

select * from renamed