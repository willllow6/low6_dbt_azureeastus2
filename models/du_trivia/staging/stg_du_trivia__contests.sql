with

source as (

    select *
    from {{ source('du_trivia', 'contests') }}

),

renamed as (

    select

        ----------  ids
        id::varchar as contest_id,
        campaignid::varchar as campaign_id,
        categoryid::varchar as category_id,
        'du' as client_id,
        'du' as tenant_id,

        ---------- strings
        'DU' as tenant_name,
        'trivia' as game_type,
        title as contest_name,
        titlear as contest_name_ar,
        description as contest_description,
        descriptionar as contest_description_ar,
        background_image,
        status::varchar as contest_status_code,

        ---------- dates
        contestdate as contest_date,

        ---------- timestamps
        convert_timezone('UTC', createdat)::timestamp_ntz as created_at,
        convert_timezone('UTC', updatedat)::timestamp_ntz as updated_at

    from source

)

select * from renamed
