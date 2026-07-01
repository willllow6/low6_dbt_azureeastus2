with

source as (

    select *
    from {{ source('bet99_bracket', 'conferences') }}

),

renamed as (

    select

        ----------  ids
        conferenceid as conference_id,

        ---------- strings
        '{{ var("bet99_bracket_current_tournament") }}' as tournament_name,
        name as conference_name,
        align as conference_align,
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
