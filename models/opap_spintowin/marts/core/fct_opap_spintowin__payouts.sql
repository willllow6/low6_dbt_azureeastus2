with

payouts as (

    select *
    from {{ ref('int_opap_spintowin__payouts') }}

)

select
    payout_id,
    entry_id,
    user_id,
    prize_tier_id,
    contest_id,
    wallet_reference_id,
    prize_type,
    prize_amount,
    payment_status,
    payment_processed_at,
    cast(convert_timezone('UTC', '{{ var("opap_spintowin_local_timezone") }}', payment_processed_at) as date) as payment_processed_date_et,
    convert_timezone('UTC', '{{ var("opap_spintowin_local_timezone") }}', payment_processed_at)::timestamp_ntz as payment_processed_at_et,
    created_at,
    cast(convert_timezone('UTC', '{{ var("opap_spintowin_local_timezone") }}', created_at) as date) as created_date_et,
    convert_timezone('UTC', '{{ var("opap_spintowin_local_timezone") }}', created_at)::timestamp_ntz as created_at_et,
    updated_at
from payouts
