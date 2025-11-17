with

source as (

    select *
    from {{ source('oilers_picks', 'pickemquestionoptions') }}

),

renamed as (

    select

        ----------  ids
        optionid as option_id,
        questionid as question_id,
        pickemid as pickem_id,

        ---------- strings
        titleen as option_title,
        descriptionen as option_description,

        ---------- numerics
        points as option_points,

        ---------- booleans
        iscorrect as is_option_correct,

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
