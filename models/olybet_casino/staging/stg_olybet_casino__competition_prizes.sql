with

source as (

    select *
    from {{ source('olybet_casino', 'competition_prize') }}

),

renamed as (

    select

        ----------  ids
        id::varchar as competition_prize_id,
        prize_id::varchar as prize_id,
        competition_id::varchar as contest_id,

        ---------- strings
        aux_prizes,

        ---------- numerics
        tier as prize_tier,
        quantity as prize_quantity,
        win_chance,

        ---------- booleans
        deleted_at is not null as is_deleted,

        ---------- timestamps
        created_at::timestamp_ntz as created_at,
        deleted_at::timestamp_ntz as deleted_at

    from source

)

select * from renamed
