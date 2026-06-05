with

payouts as (

    select *
    from {{ ref('fct_opap_spintowin__payouts') }}

),

users as (

    select
        user_id,
        sso_user_id,
        user_state
    from {{ ref('dim_opap_spintowin__users') }}

)

select
    payouts.payout_id,
    payouts.user_id,
    users.sso_user_id,
    users.user_state,
    payouts.entry_id,
    payouts.contest_id,
    payouts.wallet_reference_id,
    payouts.prize_type,
    payouts.prize_amount,
    payouts.payment_status,
    payouts.payment_processed_at,
    payouts.created_at,
    payouts.updated_at
from payouts
inner join users
    on payouts.user_id = users.user_id
