with

coin_txns as (

    select *
    from {{ ref('mart_bet365_uf__coin_transactions') }}
    where is_tester = false

),

daily_agg as (

    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', coin_transaction_created_at) as date) as date_day,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        count(distinct user_id) as active_users,
        sum(case when coin_amount > 0 then coin_amount else 0 end) as total_coins_generated,
        sum(case when coin_amount < 0 then coin_amount else 0 end) * -1 as total_coins_sunk,
        sum(coin_amount) as net_inflation
    from coin_txns
    group by 1, 2, 3, 4, 5

)

select
    date_day,
    client_id,
    tenant_id,
    tenant_name,
    game_type,
    active_users,
    total_coins_generated,
    total_coins_sunk,
    net_inflation,
    div0(total_coins_generated, active_users) as cgr_per_user,
    div0(total_coins_sunk, active_users) as csr_per_user
from daily_agg
