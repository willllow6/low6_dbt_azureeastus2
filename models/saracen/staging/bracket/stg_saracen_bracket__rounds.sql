with

source as (

    select *
    from {{ source('saracen_bracket', 'rounds') }}

),

renamed as (

    select

        ----------  ids
        roundid as round_id,

        ---------- strings
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