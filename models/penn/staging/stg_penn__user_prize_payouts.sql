with

source as (

    select *
    from {{ source('penn', 'user_prize_payouts') }}

),

renamed as (

    select

        ---------- ids
        id as payout_id,
        user_id,
        pickem_id as contest_id,
        leaderboard_id,
        wallet_transaction_id,
        wallet_reference_id,
        created_by,
        updated_by,

        ---------- strings
        prize_type,
        prize_description,
        payment_status,
        payment_error,
        notes,

        ---------- numerics
        prize_amount,
        rank,
        tied_with_count,

        ---------- dates
        cast(payment_processed_at as date) as payment_processed_date,
        cast(created_at as date) as payout_created_date,

        ---------- timestamps
        payment_processed_at,
        created_at,
        updated_at

    from source

)

select * from renamed
