with

source as (

    select *
    from {{ source('oilers_picks', 'pickemquestions') }}
    where questionstatus is null

),

renamed as (

    select
        ----------  ids
        questionid as question_id,
        pickemid as pickem_id,

        ---------- strings
        titleen as question_title,
        descriptionen as question_description,
        category as question_category,
        viewtype as question_type,

        ---------- numerics
        points as question_points,
        tiebreakscore as tiebreak_outcome,

        ---------- booleans
        isbonus as is_bonus_question,
        istiebreak as is_tiebreak_question,

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
