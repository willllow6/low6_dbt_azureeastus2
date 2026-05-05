with

owner_changes as (
    select * from {{ ref('fct_bet365_uf__card_ownership_changes') }}
),

cards as (
    select * from {{ ref('mart_bet365_uf__user_cards') }}
),

tenants as (
    select tenant_id, tenant_name from {{ ref('stg_bet365_uf__tenants') }}
),

users_from as (
    select user_id, username from {{ ref('stg_bet365_uf__users') }}
),

users_to as (
    select user_id, username from {{ ref('stg_bet365_uf__users') }}
),

coin_transactions as (
    select coin_transaction_id, coin_amount from {{ ref('fct_bet365_uf__coin_transactions') }}
),

joined as (

    select
        owner_changes.card_ownership_change_id,
        owner_changes.tenant_id,
        owner_changes.coin_transaction_id,
        owner_changes.card_id,
        owner_changes.card_ownership_changed_from_user_id,
        owner_changes.card_ownership_changed_to_user_id,
        owner_changes.client_id,
        owner_changes.card_ownership_change_event,
        owner_changes.card_ownership_change_reference,
        owner_changes.card_ownership_change_date,
        owner_changes.card_ownership_changed_at,
        tenants.tenant_name,
        cards.scoreable_name,
        cards.scoreable_team_name,
        cards.scoreable_rating,
        cards.scoreable_position,
        coin_transactions.coin_amount,
        users_from.username as card_ownership_changed_from,
        users_to.username as card_ownership_changed_to
    from owner_changes
    left join cards
        on owner_changes.card_id = cards.card_id
    left join tenants
        on owner_changes.tenant_id = tenants.tenant_id
    left join coin_transactions
        on owner_changes.coin_transaction_id = coin_transactions.coin_transaction_id
    left join users_from
        on owner_changes.card_ownership_changed_from_user_id = users_from.user_id
    left join users_to
        on owner_changes.card_ownership_changed_to_user_id = users_to.user_id

)

select * from joined
