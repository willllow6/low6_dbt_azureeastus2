with

source as (

    select *
    from {{ source('du_trivia', 'userselections') }}

),

renamed as (

    select

        ----------  ids
        {{ dbt_utils.generate_surrogate_key(['userid', 'contestid', 'questionid']) }} as selection_id,
        {{ dbt_utils.generate_surrogate_key(['userid', 'contestid']) }} as entry_id,
        userid::varchar as user_id,
        contestid::varchar as contest_id,
        questionid::varchar as question_id,
        optionid::varchar as option_id,
        'du' as client_id,
        'du' as tenant_id,

        ---------- strings
        'DU' as tenant_name,
        'trivia' as game_type,

        ---------- numerics
        points as selection_points,
        elapsedseconds as elapsed_seconds,
        clientprovidedpoints as client_provided_points,
        clientprovidedelapsedseconds as client_provided_elapsed_seconds,

        ---------- booleans
        iscorrect as is_correct,

        ---------- timestamps
        convert_timezone('UTC', startedat)::timestamp_ntz as started_at,
        convert_timezone('UTC', selectedat)::timestamp_ntz as selected_at,
        convert_timezone('UTC', updatedat)::timestamp_ntz as updated_at

    from source

)

select * from renamed
