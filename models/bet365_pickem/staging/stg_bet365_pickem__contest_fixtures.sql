with

source as (

    select *
    from {{ source('bet365_pickem', 'contest_fixtures') }}

),

renamed as (

    select

        ----------  ids
        id::varchar as contest_fixture_id,
        contest_id::varchar as contest_id,
        fixture_id::varchar as fixture_id,
        definitions_version_id::varchar as definitions_version_id,
        'bet365' as client_id,

        ---------- strings
        'pickem' as game_type,

        ---------- numerics
        position as fixture_position,

        ---------- booleans
        is_main_event,

        ---------- timestamps
        early_lock_at::timestamp_ntz as early_lock_at,
        lock_at::timestamp_ntz as lock_at,
        locked_at::timestamp_ntz as locked_at,
        created_at::timestamp_ntz as created_at

    from source

)

select * from renamed
