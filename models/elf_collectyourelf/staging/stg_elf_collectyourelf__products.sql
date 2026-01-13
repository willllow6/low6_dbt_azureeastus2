with

source as (

    select *
    from {{ source('collectyourelf','product') }}

),

renamed as (

    select

        ----------  ids
        product_id,

        ---------- strings
        product_name,
        description as product_description,

        ---------- numerics
    
        ---------- booleans
        is_enabled,

        ---------- dates

        ---------- timestamps
        created_at

    from source

)

select * from renamed