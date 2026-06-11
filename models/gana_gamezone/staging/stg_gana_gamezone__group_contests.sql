with

source as (

    select * from {{ source('gana_gamezone', 'group_matches') }}

),

renamed as (

    select

        ---------- ids
        id          as group_id,

        ---------- strings
        name        as group_name,
        description,

        ---------- semi-structured
        countries,

        ---------- timestamps
        cast(created_at as timestamp_ntz)   as created_at,
        cast(updated_at as timestamp_ntz)   as updated_at

    from source

)

select * from renamed
