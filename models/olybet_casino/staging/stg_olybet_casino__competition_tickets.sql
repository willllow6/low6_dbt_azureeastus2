with

source as (

    select *
    from {{ source('olybet_casino', 'competition_ticket') }}

),

renamed as (

    select

        ----------  ids
        id::varchar as ticket_id,
        competition_id::varchar as contest_id,
        entry_id::varchar as entry_id,
        -- references competition_prize.id (the tier won), NOT prize.id
        -- directly — confirmed against source data (100% match against
        -- competition_prize vs ~2% coincidental match against prize)
        prize_id::varchar as competition_prize_id,

        ---------- booleans
        prize_id is not null as is_winner,
        played_at is not null as is_played,

        ---------- dates
        cast(played_at as date) as played_date,

        ---------- timestamps
        played_at::timestamp_ntz  as played_at,
        created_at::timestamp_ntz as created_at,
        deleted_at::timestamp_ntz as deleted_at,
        greatest(
            coalesce(played_at,  '1900-01-01'::timestamp_ntz),
            coalesce(created_at, '1900-01-01'::timestamp_ntz),
            coalesce(deleted_at, '1900-01-01'::timestamp_ntz)
        )::timestamp_ntz as updated_at

    from source

)

select * from renamed
