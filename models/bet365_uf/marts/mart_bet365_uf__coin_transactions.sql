with

coin_transactions as (
    select * from {{ ref('fct_bet365_uf__coin_transactions') }}
),

users as (
    select user_id, username, is_enabled, is_tester from {{ ref('stg_bet365_uf__users') }}
),

tenants as (
    select tenant_id, tenant_name from {{ ref('stg_bet365_uf__tenants') }}
),

card_ownership_changes as (
    select * from {{ ref('fct_bet365_uf__card_ownership_changes') }}
),

scoreables as (
    select scoreable_id, scoreable_name, scoreable_rating from {{ ref('stg_bet365_uf__scoreables') }}
),

joined as (

    select
        coin_transactions.coin_transaction_id,
        coin_transactions.pair_coin_transaction_id,
        coin_transactions.tenant_id,
        coin_transactions.user_id,
        coin_transactions.client_id,
        coin_transactions.game_type,
        coin_transactions.coin_transaction_type,
        coin_transactions.coin_amount,
        coin_transactions.coin_transaction_reference,
        coin_transactions.is_sink,
        coin_transactions.coin_transaction_created_at,
        coin_transactions.coin_transaction_created_date,
        tenants.tenant_name,
        users.username,
        users.is_enabled as is_user_enabled,
        users.is_tester,
        coalesce(
            s1.scoreable_name,
            s2.scoreable_name
        ) as scoreable_name,
        coalesce(
            s1.scoreable_rating,
            s2.scoreable_rating
        ) as scoreable_rating,
        coalesce(
            c1.card_ownership_changed_from_user_id,
            c2.card_ownership_changed_to_user_id
        ) as transacted_with_user_id
    from coin_transactions
    left join users
        on coin_transactions.user_id = users.user_id
    left join tenants
        on coin_transactions.tenant_id = tenants.tenant_id
    left join card_ownership_changes as c1
        on coin_transactions.coin_transaction_id = c1.coin_transaction_id
    left join card_ownership_changes as c2
        on coin_transactions.pair_coin_transaction_id = c2.coin_transaction_id
    left join scoreables as s1
        on c1.scoreable_id = s1.scoreable_id
    left join scoreables as s2
        on c2.scoreable_id = s2.scoreable_id

)

select * from joined
