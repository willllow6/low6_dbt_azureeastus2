with

owner_changes as (
    select * from {{ ref('stg_bet365_uf__card_ownership_changes') }}
),

users as (
    select user_id, username, is_tester from {{ ref('stg_bet365_uf__users') }}
),

coin_transactions as (
    select coin_transaction_id, coin_amount from {{ ref('stg_bet365_uf__coin_transactions') }}
),

cards as (
    select * from {{ ref('mart_bet365_uf__user_cards') }}
),

final as (

    select
        co.card_ownership_change_id,
        co.coin_transaction_id,
        co.card_id,
        co.card_ownership_changed_from_user_id,
        co.card_ownership_changed_to_user_id,
        co.card_ownership_change_event,
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', co.card_ownership_changed_at) as date) as card_ownership_change_date,
        co.card_ownership_changed_at,
        cards.scoreable_name,
        cards.scoreable_team_name,
        cards.scoreable_rating,
        cards.scoreable_position,
        ct.coin_amount,
        u1.username as card_ownership_changed_from,
        u2.username as card_ownership_changed_to,
        u1.is_tester as seller_is_tester,
        u2.is_tester as purchaser_is_tester
    from owner_changes as co
    left join cards
        on co.card_id = cards.card_id
    left join coin_transactions as ct
        on co.coin_transaction_id = ct.coin_transaction_id
    left join users as u1
        on co.card_ownership_changed_from_user_id = u1.user_id
    left join users as u2
        on co.card_ownership_changed_to_user_id = u2.user_id

)

select * from final
