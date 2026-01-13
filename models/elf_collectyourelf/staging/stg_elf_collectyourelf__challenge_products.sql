with

source as (

    select *
    from {{ source('collectyourelf','challenge_product') }}

),

renamed as (

    select

        ----------  ids
        challenge_id,
        product_id,

        ---------- strings

        ---------- numerics
        "order" as challenge_product_order,
        ---------- booleans

        ---------- dates

        ---------- timestamps
        created_at
    from source

)

select * from renamed