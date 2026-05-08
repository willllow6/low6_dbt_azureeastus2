with

payouts as (

    select *
    from {{ ref('fct_penn__payouts') }}

),

users as (

    select
        user_id,
        sso_user_id,
        username
    from {{ ref('dim_penn__users') }}

),

joined as (

    select
        p.payout_id,
        p.user_id,
        u.sso_user_id,
        u.username,
        p.contest_id,
        p.prize_type,
        p.prize_amount,
        p.prize_description,
        p.rank,
        p.tied_with_count,
        p.payment_status,
        p.payment_processed_at,
        p.payment_processed_date,
        p.payment_processed_date_et,
        p.payout_created_date,
        p.payout_created_date_et,
        p.created_at,
        p.updated_at
    from payouts p
    left join users u
        on p.user_id = u.user_id

)

select * from joined
