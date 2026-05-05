with

source as (

    select *
    from {{ source('bet365_uf', 'PACKS') }}

),

renamed as (

    select

        ----------  ids
        PackId as pack_id,
        TenantId as tenant_id,
        UserId as user_id,
        'bet365' as client_id,

        ---------- strings
        REGEXP_REPLACE(Name, '[0-9]', '') as pack_name,
        REGEXP_REPLACE(PackType, '[0-9]', '') as pack_type,

        ---------- booleans
        IsNew as is_new,

        ---------- dates
        cast(AssignedAt as date) as pack_assigned_date,
        cast(CreatedAt as date) as pack_created_date,

        ---------- timestamps
        AssignedAt as pack_assigned_at,
        CreatedAt as pack_created_at

    from source

)

select * from renamed
