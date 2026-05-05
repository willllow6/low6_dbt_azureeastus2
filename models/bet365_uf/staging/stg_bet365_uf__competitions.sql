with

source as (

    select *
    from {{ source('bet365_uf', 'COMPETITIONS') }}

),

renamed as (

    select

        ----------  ids
        CompetitionId as competition_id,
        TenantId as tenant_id,
        'bet365' as client_id,

        ---------- strings
        ExternalId as external_id,
        Name as competition_name,
        Sport as sport,

        ---------- booleans
        IsInternational as is_international_competition,

        ---------- dates
        cast(CreatedAt as date) as created_date,

        ---------- timestamps
        CreatedAt as created_at

    from source

)

select * from renamed
