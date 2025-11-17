with

source as (

    select *
    from {{ source('oilers_picks', 'userpickempoints') }}

),

renamed as (

    select

        ----------  ids
        userpickempointsid as entry_score_id,
        userid as user_id,
        pickemid as pickem_id,

        ---------- strings

        ---------- numerics
        points as entry_points,

        ---------- booleans

        ---------- dates
        cast(createdat as date) as created_date,
        cast(
            convert_timezone('UTC', 'America/New_York', createdat) as date
        ) as created_date_et,

        ---------- timestamps
        createdat as created_at,
        convert_timezone('UTC', 'America/New_York', createdat) as created_at_et

    from source

)

select * from renamed
