with

source as (

    select *
    from {{ source('oilers_picks', 'userselections') }}

),

renamed as (

    select

        ----------  ids
        selectionid as selection_id,
        userid as user_id,
        pickemid as pickem_id,
        questionid as question_id,
        optionid as option_id,

        ---------- strings

        ---------- numerics
        try_to_number(substr(customvalue, 0, 5)) as tiebreak_prediction,

        ---------- booleans

        ---------- dates
        cast(
            convert_timezone('UTC', 'America/New_York', createdat) as date
        ) as created_date_et,
        createdat as created_at,

        ---------- timestamps
        cast(createdat as date) as created_date,
        convert_timezone('UTC', 'America/New_York', createdat) as created_at_et

    from source

)

select * from renamed
