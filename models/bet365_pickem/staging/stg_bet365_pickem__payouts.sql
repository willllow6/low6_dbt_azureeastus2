with

source as (

    select *
    from {{ source('bet365_pickem', 'payouts') }}

),

renamed as (

    select

        ----------  ids
        id::varchar as payout_id,
        entry_id::varchar as entry_id,
        prize_run_id::varchar as prize_run_id,
        'bet365' as client_id,

        ---------- strings
        'pickem' as game_type,
        prize_pool,
        currency,
        state as payout_status,
        place_balance_type,
        jackpot_balance_type,
        case
            when place_balance_type = 'real' then 'CASH'
            else upper(place_balance_type)
        end as prize_type,
        resolved_by,
        resolution_note,

        ---------- numerics
        place as prize_place,
        place_amount,
        jackpot_amount,
        amount as prize_amount,

        ---------- booleans
        resolved_at is not null as is_resolved,

        ---------- timestamps
        created_at::timestamp_ntz as created_at,
        released_at::timestamp_ntz as released_at,
        resolved_at::timestamp_ntz as resolved_at

    from source

)

select * from renamed
