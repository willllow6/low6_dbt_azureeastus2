with

source as (

    select *
    from {{ source('penn', 'tenants') }}

),

renamed as (

    select

        ---------- ids
        id as tenant_id,

        ---------- strings
        code as tenant_code,
        name_en as tenant_name,
        title,
        'penn' as client_id,

        ---------- timestamps
        cast(created_at as timestamp_ntz) as created_at,
        cast(updated_at as timestamp_ntz) as updated_at

    from source

)

select * from renamed
