with

source as (

    select *
    from {{ source('nc_trivia', 'contests') }}

),

renamed as (

    select

        ----------  ids
        id as contest_id,

        ---------- strings
        title as contest_title,
        description as contest_description,
        status as contest_status,

        ---------- numerics

        ---------- booleans

        ---------- dates
        contestdate as contest_start_date,
        convert_timezone('UTC','Australia/Sydney', contestdate) as contest_start_date_aet,

        ---------- timestamps
        createdat as created_at,
        updatedat as updated_at

    from source

)

select * from renamed
