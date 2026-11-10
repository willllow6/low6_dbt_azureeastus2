with

source as (

    select *
    from {{ source('du_trivia', 'userparticipations') }}

),

renamed as (

    select

        ----------  ids
        {{ dbt_utils.generate_surrogate_key(['userid', 'contestid']) }} as entry_id,
        userid::varchar as user_id,
        contestid::varchar as contest_id,
        'du' as client_id,
        'du' as tenant_id,

        ---------- strings
        'DU' as tenant_name,
        'trivia' as game_type,

        ---------- numerics
        points as entry_points,

        ---------- booleans
        iscompleted as is_completed,
        hasmaxpoints as has_max_points,

        ---------- timestamps
        joinedat::timestamp_ntz as entered_at,
        completedat::timestamp_ntz as completed_at,
        updatedat::timestamp_ntz as updated_at

    from source

)

select * from renamed
