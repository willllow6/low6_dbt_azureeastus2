with

source as (

    select *
    from {{ source('saracen_bracket_nbafinals2026', 'rounds') }}

),

renamed as (

    select

        ----------  ids
        roundid as round_id,

        ---------- strings
        'nba_finals_2026' as tournament_name,
        name as round_name,
        competition as competition_name,

        ---------- numerics
        point as round_points,

        ---------- booleans

        ---------- dates
        cast(createdat as date) as created_date,

        ---------- timestamps
        createdat as created_at

    from source

)

select * from renamed
