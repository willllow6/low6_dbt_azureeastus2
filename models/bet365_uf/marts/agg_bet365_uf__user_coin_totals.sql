with

coin_transactions as (

    select * from {{ ref('stg_bet365_uf__coin_transactions') }}

),

users as (

    select * from {{ ref('stg_bet365_uf__users') }}

),

final as (

    select
        ct.user_id,
        u.username,
        u.is_tester,
        sum(coin_amount) as balance,
        sum(case when coin_transaction_type = 'Pack Purchase' then 1 else 0 end) as packs_purchased_count,
        sum(case when coin_transaction_type = 'Pack Purchase' then coin_amount * -1 else 0 end) as packs_purchased_amount,
        sum(case when coin_transaction_type = 'Card Purchase' then 1 else 0 end) as cards_purchased_count,
        sum(case when coin_transaction_type = 'Card Purchase' then coin_amount * -1 else 0 end) as cards_purchased_amount,
        sum(case when coin_transaction_type = 'Card Sale' then 1 else 0 end) as cards_sold_count,
        sum(case when coin_transaction_type = 'Card Sale' then coin_amount else 0 end) as cards_sold_amount,
        sum(case when coin_transaction_type = 'Coin Purchase' then 1 else 0 end) as coins_purchased_count,
        sum(case when coin_transaction_type = 'Coin Purchase' then coin_amount else 0 end) as coins_purchased_amount,
        sum(case when coin_transaction_type = 'Challenge Reward' then 1 else 0 end) as challenge_reward_count,
        sum(case when coin_transaction_type = 'Challenge Reward' then coin_amount else 0 end) as challenge_reward_amount,
        sum(case when coin_transaction_type = 'Friend Referral Reward' then 1 else 0 end) as referral_reward_count,
        sum(case when coin_transaction_type = 'Friend Referral Reward' then coin_amount else 0 end) as referral_reward_amount,
        sum(case when coin_transaction_type = 'Registration Reward' then 1 else 0 end) as registration_reward_count,
        sum(case when coin_transaction_type = 'Registration Reward' then coin_amount else 0 end) as registration_reward_amount,
        sum(case when coin_transaction_type = 'Login Reward' then 1 else 0 end) as login_reward_count,
        sum(case when coin_transaction_type = 'Login Reward' then coin_amount else 0 end) as login_reward_amount,
        sum(case when coin_transaction_type = 'Pack Purchase' and coin_amount = -5000 then 1 else 0 end) as legends_packs_purchased_count,
        sum(case when coin_transaction_type = 'Pack Purchase' and coin_amount = -5000 then coin_amount * -1 else 0 end) as legends_packs_purchased_amount
    from coin_transactions as ct
    left join users as u
        on ct.user_id = u.user_id
    group by 1, 2, 3

)

select * from final
