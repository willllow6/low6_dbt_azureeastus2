with

source as (

    select *
    from {{ source('nc_trivia', 'user_selections') }}

),

renamed as (

    select

        ----------  ids
        contestid as contest_id,
        userid as user_id,
        questionid as question_id,
        optionid as option_id,

        ---------- strings

        ---------- numerics
        elapsedseconds as elapsed_time_seconds,
        points as selection_points,

        ---------- booleans
        iscorrect as is_correct,

        ---------- dates

        ---------- timestamps
        startedat as started_at,
        convert_timezone('UTC','Australia/Sydney',startedat) as started_at_aet,
        selectedat as selected_at,
        convert_timezone('UTC','Australia/Sydney',selectedat) as selected_at_aet,
        updatedat as updated_at

    from source

)

select * from renamed