with

source as (

    select *
    from {{ source('bet365_pickem', 'standings') }}

),

renamed as (

    select

        ----------  ids
        entry_id::varchar as entry_id,
        contest_id::varchar as contest_id,
        'bet365' as client_id,

        ---------- strings
        'pickem' as game_type,
        bonuses,

        ---------- numerics
        pick_points,
        bonus_points,
        score,
        rank as entry_rank,

        ---------- timestamps
        updated_at::timestamp_ntz as updated_at

    from source

)

select * from renamed
