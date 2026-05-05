with

user_cards as (

    select *
    from {{ ref('mart_bet365_uf__user_cards') }}
    where is_tester = false

),

coin_transactions as (

    select *
    from {{ ref('mart_bet365_uf__coin_transactions') }}
    where is_tester = false

),

user_gold_cards as (

    select
        user_id,
        count(*) as gold_cards
    from user_cards
    where scoreable_rating = 'Gold'
    group by 1

),

user_gold_ranks as (

    select
        user_id,
        gold_cards,
        round(floor(percent_rank() over (order by gold_cards desc) * 100 / 10), 1) + 1 as gold_card_decile
    from user_gold_cards

),

daily_active_users_gold_decile as (

    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', t.coin_transaction_created_at) as date) as coin_transaction_created_date,
        r.gold_card_decile,
        count(distinct t.user_id) as daily_active_users
    from coin_transactions as t
    inner join user_gold_ranks as r
        on t.user_id = r.user_id
    group by 1, 2

),

type_cgr as (

    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', ct.coin_transaction_created_at) as date) as coin_transaction_created_date,
        r.gold_card_decile,
        ct.coin_transaction_type,
        sum(case when coin_amount > 0 then coin_amount else 0 end) as total_coins_generated,
        sum(case when coin_amount < 0 then coin_amount else 0 end) * -1 as total_coins_sunk
    from coin_transactions as ct
    inner join user_gold_ranks as r
        on ct.user_id = r.user_id
    group by 1, 2, 3

),

joined as (

    select
        t.coin_transaction_created_date,
        t.coin_transaction_type,
        t.gold_card_decile,
        u.daily_active_users,
        t.total_coins_generated,
        t.total_coins_sunk,
        div0(t.total_coins_generated, u.daily_active_users) as cgr_per_user,
        div0(t.total_coins_sunk, u.daily_active_users) as csr_per_user
    from type_cgr as t
    left join daily_active_users_gold_decile as u
        on t.coin_transaction_created_date = u.coin_transaction_created_date
        and t.gold_card_decile = u.gold_card_decile

)

select * from joined
