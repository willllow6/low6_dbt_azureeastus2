with

source as (

    select *
    from {{ source('bet365_uf', 'SCOREABLE_COMPETITIONS') }}

),

renamed as (

    select

        ----------  ids
        ScoreableCompetitionId as scoreable_competition_id,
        ScoreableId as scoreable_id,
        CompetitionId as competition_id,
        'bet365' as client_id,

        ---------- dates
        cast(CreatedAt as date) as created_date,

        ---------- timestamps
        CreatedAt as created_at

    from source

)

select * from renamed
