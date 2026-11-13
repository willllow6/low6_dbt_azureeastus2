with

source as (

    select *
    from {{ source('bet365_pickem', 'picks') }}

),

renamed as (

    select

        ----------  ids
        id::varchar as selection_id,
        entry_id::varchar as entry_id,
        contest_id::varchar as contest_id,
        contest_fixture_id::varchar as contest_fixture_id,
        question_id::varchar as question_id,
        option_id::varchar as option_id,
        'bet365' as client_id,

        ---------- strings
        'pickem' as game_type,
        input as pick_input,
        price_fractional,
        price_american,

        ---------- numerics
        price_decimal,
        returns as potential_returns,

        ---------- timestamps
        saved_at::timestamp_ntz as selected_at,
        created_at::timestamp_ntz as created_at

    from source

)

select * from renamed
