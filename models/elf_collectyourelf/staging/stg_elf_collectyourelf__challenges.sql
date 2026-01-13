with

source as (

    select *
    from {{ source('collectyourelf','challenge') }}

),

renamed as (

    select

        ----------  ids
        challenge_id,

        ---------- strings
        challenge_name,
        description as challenge_description,

        ---------- numerics
        challenge_order,
    
        ---------- booleans
        is_enabled,

        ---------- dates

        ---------- timestamps
        created_at

    from source

)

select * from renamed