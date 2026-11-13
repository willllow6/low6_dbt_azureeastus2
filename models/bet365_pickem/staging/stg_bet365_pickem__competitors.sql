with

source as (

    select *
    from {{ source('bet365_pickem', 'catalogue_competitors') }}

),

renamed as (

    select

        ----------  ids
        id::varchar as competitor_id,
        merged_into_id::varchar as merged_into_competitor_id,
        'bet365' as client_id,

        ---------- strings
        'pickem' as game_type,
        trim(name) as competitor_name,
        attributes as competitor_attributes,
        headshot_path,

        ---------- booleans
        merged_into_id is null as is_active,

        ---------- timestamps
        created_at::timestamp_ntz as created_at,
        updated_at::timestamp_ntz as updated_at,
        merged_at::timestamp_ntz as merged_at

    from source

)

select * from renamed
