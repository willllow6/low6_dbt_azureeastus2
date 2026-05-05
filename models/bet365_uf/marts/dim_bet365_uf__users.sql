with

users as (

    select * 
    from {{ ref('stg_bet365_uf__users') }}

),

purchases as (

    select
        user_id,
        count(*) as purchases,
        min(purchased_at) as first_purchased_at,
        min(cast(purchased_at as date)) as first_purchase_date,
        sum(purchase_price) as gross_revenue
    from {{ ref('fct_bet365_uf__app_store_purchases') }}
    group by 1

),

pack_purchases as (

    select
        user_id,
        count(*) as pack_purchases
    from {{ ref('fct_bet365_uf__coin_transactions') }}
    where coin_transaction_type = 'Pack Purchase'
    group by 1

),

entries as (

    select
        user_id,
        count(*) as entry_count
    from {{ ref('fct_bet365_uf__entries') }}
    group by 1

),

referrals as (

    select
        user_id,
        count(*) as referral_count
    from {{ ref('stg_bet365_uf__referrals') }}
    group by 1

),

active_days as (

    select
        user_id,
        max(user_active_day_number) as active_days
    from {{ ref('fct_bet365_uf__user_active_days') }}
    group by 1

),

joined as (

    select
        users.user_id,
        users.client_id,
        users.email,
        users.username,
        users.first_name,
        users.last_name,
        users.user_state,
        users.user_age,
        users.user_age_band,
        users.user_generation,
        users.mobile_number,
        users.is_deleted,
        users.is_enabled,
        users.is_playable,
        users.is_tester,
        users.has_agreed_privacy_policy,
        users.has_accepted_terms_and_conditions,
        users.has_confirmed_18_plus,
        users.has_consented_marketing,
        users.has_completed_profile,
        users.birth_date,
        users.registered_date,
        users.last_login_at,
        users.registered_at,
        users.registration_type,
        users.game_type,
        coalesce(entries.entry_count, 0) as entry_count,
        coalesce(active_days.active_days, 0) as active_days,
        case when active_days.active_days > 1 then true else false end as is_returning_user,
        coalesce(referrals.referral_count, 0) as referral_count,
        case when referrals.referral_count > 0 then true else false end as is_referring_user,
        coalesce(pack_purchases.pack_purchases, 0) as pack_purchases,
        case when pack_purchases.pack_purchases > 0 then true else false end as has_purchased_pack,
        purchases.gross_revenue is not null as is_paying_user,
        coalesce(purchases.purchases, 0) as purchases,
        coalesce(purchases.gross_revenue, 0) as gross_revenue,
        purchases.first_purchased_at,
        purchases.first_purchase_date,
        datediff('second', users.registered_at, purchases.first_purchased_at) / 3600 as hours_to_first_purchase
    from users
    left join purchases
        on users.user_id = purchases.user_id
    left join entries
        on users.user_id = entries.user_id
    left join active_days
        on users.user_id = active_days.user_id
    left join pack_purchases
        on users.user_id = pack_purchases.user_id
    left join referrals
        on users.user_id = referrals.user_id

),

final as (

    select
        *,
        case
            when gross_revenue <= 0 or gross_revenue is null then ''
            when gross_revenue < 5 then '<5'
            when gross_revenue < 10 then '5-9.99'
            when gross_revenue < 25 then '10-24.99'
            when gross_revenue < 50 then '25-49.99'
            when gross_revenue < 75 then '50-74.99'
            when gross_revenue < 100 then '75-99.99'
            else '100+'
        end as gross_revenue_band,
        case
            when hours_to_first_purchase is null then ''
            when hours_to_first_purchase < 1 then '1st hour'
            when hours_to_first_purchase < 12 then '2-12 hours'
            when hours_to_first_purchase < 24 then '12-24 hours'
            when hours_to_first_purchase < 48 then '24-48 hours'
            when hours_to_first_purchase < 168 then '3-7 days'
            else '7+ days'
        end as time_to_first_purchase_band
    from joined

)

select * from final
