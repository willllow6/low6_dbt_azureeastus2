with

source as (

    select *
    from {{ source('bet365_uf', 'FIXTURES') }}

),

renamed as (

    select

        ----------  ids
        FixtureId as fixture_id,
        TenantId as tenant_id,
        CompetitionId as competition_id,
        'bet365' as client_id,

        ---------- strings
        ExternalId as fixture_external_id,
        Name as fixture_name,
        Status as fixture_status,

        ---------- dates
        cast(StartDateUtc as date) as fixture_start_date,
        cast(CreatedAt as date) as fixture_created_date,

        ---------- timestamps
        StartDateUtc as fixture_starts_at,
        CreatedAt as fixture_created_at

    from source

)

select * from renamed
