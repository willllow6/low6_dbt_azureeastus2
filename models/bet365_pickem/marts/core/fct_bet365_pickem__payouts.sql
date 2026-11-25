with

payouts as (

    select *
    from {{ ref('stg_bet365_pickem__payouts') }}

),

entries as (

    select *
    from {{ ref('fct_bet365_pickem__entries') }}

)

select
    payouts.payout_id,
    payouts.entry_id,
    entries.user_id,
    entries.contest_id,
    payouts.prize_run_id,

    payouts.client_id,
    entries.tenant_id,
    entries.tenant_name,
    payouts.game_type,

    payouts.prize_pool,
    payouts.prize_type,
    payouts.payout_status,
    payouts.is_resolved,
    payouts.prize_place,

    payouts.place_amount,
    payouts.jackpot_amount,
    payouts.prize_amount,
    payouts.currency,

    payouts.released_at as paid_at,
    cast(
        convert_timezone('UTC', '{{ var("local_timezone") }}', payouts.released_at) as date
    ) as paid_date_et,

    payouts.created_at,
    payouts.released_at,
    payouts.resolved_at

from payouts
inner join entries
    on payouts.entry_id = entries.entry_id
