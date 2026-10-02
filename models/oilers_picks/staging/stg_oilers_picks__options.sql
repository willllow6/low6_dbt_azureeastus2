with

source as (

    select *
    from {{ source('oilers_picks', 'pickemquestionoptions') }}

),

renamed as (

    select

        ----------  ids
        optionid::varchar as option_id,
        questionid::varchar as question_id,
        pickemid::varchar as contest_id,
        'oilers' as client_id,
        'oilers' as tenant_id,

        ---------- strings
        'Edmonton Oilers' as tenant_name,
        'pickem' as game_type,
        titleen as option_title,
        descriptionen as option_description,

        ---------- numerics
        points as option_points,
        bonuspoints as bonus_points,
        optionorder as option_order,

        ---------- booleans
        iscorrect as is_correct,

        ---------- timestamps
        createdat as created_at,
        startutc as starts_at

    from source

)

select * from renamed
