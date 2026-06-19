with

source as (

    select *
    from {{ source('saracen_bracket', 'conferences') }}

),

renamed as (

    select

        ----------  ids
        conferenceid as conference_id,

        ---------- strings
        '{{ var("saracen_bracket_current_tournament") }}' as tournament_name,
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