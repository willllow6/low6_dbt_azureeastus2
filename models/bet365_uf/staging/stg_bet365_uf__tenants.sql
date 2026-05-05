with

source as (

    select *
    from {{ source('bet365_uf', 'TENANTS') }}

),

renamed as (

    select

        ----------  ids
        TenantId as tenant_id,
        'bet365' as client_id,

        ---------- strings
        Name as tenant_name,
        SportType as sport_type,

        ---------- dates

        ---------- timestamps
        CreatedAt as created_at

    from source

)

select * from renamed
