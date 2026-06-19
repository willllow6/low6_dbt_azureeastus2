with

source as (

    select *
    from {{ source('saracen_bracket_nbafinals2026', 'conferences') }}

),

renamed as (

    select

        ----------  ids
        conferenceid as conference_id,

        ---------- strings
        'nba_finals_2026' as tournament_name,
        name as conference_name,
        competition as competition_name,

        ---------- numerics

        ---------- booleans

        ---------- dates
        cast(createdat as date) as created_date,

        ---------- timestamps
        createdat as created_at

    from source

)

select * from renamed
