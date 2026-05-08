with

payouts as (

    select *
    from {{ ref('stg_penn__user_prize_payouts') }}

)

select
    payout_id,
    user_id,
    contest_id,
    wallet_reference_id,
    prize_type,
    prize_amount,
    prize_description,
    rank,
    tied_with_count,
    payment_status,
    payment_processed_at,
    payment_processed_date,
    cast(convert_timezone('UTC', '{{ var("local_timezone") }}', payment_processed_at) as date) as payment_processed_date_et,
    payout_created_date,
    cast(convert_timezone('UTC', '{{ var("local_timezone") }}', created_at) as date) as payout_created_date_et,
    created_at,
    updated_at
from payouts
