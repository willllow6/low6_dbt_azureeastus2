with

source as (

    select *
    from {{ source('olybet_casino', 'prize') }}

),

renamed as (

    select

        ----------  ids
        id::varchar as prize_id,

        ---------- strings
        name as prize_name,
        description as prize_description,
        img as prize_image_url,
        type as prize_type,

        ---------- numerics
        value as prize_value,

        ---------- booleans
        deleted_at is not null as is_deleted,

        ---------- timestamps
        expires_at::timestamp_ntz as prize_expires_at,
        created_at::timestamp_ntz as created_at,
        deleted_at::timestamp_ntz as deleted_at

    from source

)

select * from renamed
