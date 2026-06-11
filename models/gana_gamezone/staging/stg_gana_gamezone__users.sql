with

source as (

    select * from {{ source('gana_gamezone', 'users') }}

),

renamed as (

    select

        ---------- ids
        id                          as user_id,
        clerk_user_id,

        ---------- strings
        'gana'                      as client_id,
        'gana'                      as tenant_id,
        'Gana'                      as tenant_name,
        'form'                      as registration_type,
        email,
        first_name,
        last_name,
        image_url,

        ---------- booleans

        ---------- timestamps
        cast(created_at as timestamp_ntz)   as registered_at,
        cast(created_at as timestamp_ntz)   as created_at,
        cast(updated_at as timestamp_ntz)   as updated_at

    from source

)

select * from renamed
