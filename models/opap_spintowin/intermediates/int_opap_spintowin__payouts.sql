with

prize_awards as (

    select *
    from {{ ref('stg_opap_spintowin__prize_awards') }}

),

prize_tiers as (

    select
        prize_tier_id,
        contest_id,
        prize_type,
        prize_amount
    from {{ ref('stg_opap_spintowin__prize_tiers') }}

),

entries as (

    select
        entry_id,
        contest_id
    from {{ ref('stg_opap_spintowin__entries') }}

),

joined as (

    select
        pa.prize_award_id                   as payout_id,
        pa.entry_id,
        pa.user_id,
        pa.prize_tier_id,
        coalesce(e.contest_id, pt.contest_id) as contest_id,
        pa.wallet_reference_id,
        pt.prize_type,
        pt.prize_amount,
        pa.payment_status,
        pa.payment_processed_at,
        pa.created_at,
        pa.updated_at

    from prize_awards pa
    left join prize_tiers pt
        on pa.prize_tier_id = pt.prize_tier_id
    left join entries e
        on pa.entry_id = e.entry_id

)

select * from joined
