with

source as (

    select *
    from {{ source('du_trivia', 'campaigncategories') }}

),

renamed as (

    select

        ----------  ids
        id::varchar as category_id,
        campaignid::varchar as campaign_id,
        'du' as client_id,
        'du' as tenant_id,

        ---------- strings
        'DU' as tenant_name,
        'trivia' as game_type,

        ---------- semi-structured
        content as category_content,

        ---------- timestamps
        createdat::timestamp_ntz as created_at,
        updatedat::timestamp_ntz as updated_at

    from source

)

select * from renamed
