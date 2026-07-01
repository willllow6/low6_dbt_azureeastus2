with

source as (

    select *
    from {{ source('bet99_bracket', 'rounds') }}

),

renamed as (

    select

        ----------  ids
        roundid as round_id,

        ---------- strings
        '{{ var("bet99_bracket_current_tournament") }}' as tournament_name,
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
