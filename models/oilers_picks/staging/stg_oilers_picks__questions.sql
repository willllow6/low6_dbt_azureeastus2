with

source as (

    select *
    from {{ source('oilers_picks', 'pickemquestions') }}

),

renamed as (

    select

        ----------  ids
        questionid::varchar as question_id,
        pickemid::varchar as contest_id,
        'oilers' as client_id,
        'oilers' as tenant_id,

        ---------- strings
        'Edmonton Oilers' as tenant_name,
        'pickem' as game_type,
        titleen as question_title,
        descriptionen as question_description,
        category as question_category,
        viewtype as question_view_type,
        questiontype as question_type,
        questionstatus as question_status,

        ---------- numerics
        points as question_points,
        bonuspoints as bonus_points,
        tiebreakscore as tiebreak_outcome,
        questionorder as question_order,

        ---------- booleans
        questionstatus is null as is_active,
        isbonus as is_bonus_question,
        istiebreak as is_tiebreak_question,

        ---------- timestamps
        createdat as created_at,
        startutc as starts_at

    from source

)

select * from renamed
