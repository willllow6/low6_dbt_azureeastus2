with

source as (

    select *
    from {{ source('oilers_picks', 'userselections') }}

),

renamed as (

    select

        ----------  ids
        selectionid::varchar as selection_id,
        userid::varchar as user_id,
        pickemid::varchar as contest_id,
        questionid::varchar as question_id,
        optionid::varchar as option_id,
        'oilers' as client_id,
        'oilers' as tenant_id,

        ---------- strings
        'Edmonton Oilers' as tenant_name,
        'pickem' as game_type,
        customvalue as custom_value,
        bonusselectionvalue as bonus_selection_value,

        ---------- numerics
        try_to_number(substr(customvalue, 0, 5)) as tiebreak_prediction,

        ---------- timestamps
        createdat as selected_at

    from source

)

select * from renamed
