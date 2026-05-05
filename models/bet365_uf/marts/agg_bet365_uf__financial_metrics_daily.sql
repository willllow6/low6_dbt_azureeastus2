with

app_store_purchases as (
    select * from {{ ref('mart_bet365_uf__app_store_purchases') }}
    where is_tester = false
),

coin_transactions as (
    select * from {{ ref('mart_bet365_uf__coin_transactions') }}
    where is_tester = false
),

daily_revenue as (

    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', purchased_at) as date) as date_day,
        client_id,
        'bet365' as tenant_id,
        'Bet365' as tenant_name,
        game_type,
        coalesce(purchase_currency, 'USD') as currency,
        sum(purchase_price) as gross_revenue
    from app_store_purchases
    group by 1, 2, 3, 4, 5, 6

),

-- Virtual currency prizes are not real-money costs; treated as indicative only
daily_prizes as (

    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', coin_transaction_created_at) as date) as date_day,
        client_id,
        tenant_id,
        coalesce(tenant_name, client_id) as tenant_name,
        game_type,
        'USD' as currency,
        0 as gross_prizes
    from coin_transactions
    where coin_transaction_type in (
        'Leaderboard Reward',
        'Division Leaderboard Reward',
        'Challenge Reward'
    )
    group by 1, 2, 3, 4, 5, 6

),

joined as (

    select
        coalesce(dr.date_day, dp.date_day) as date_day,
        coalesce(dr.client_id, dp.client_id) as client_id,
        coalesce(dr.tenant_id, dp.tenant_id) as tenant_id,
        coalesce(dr.tenant_name, dp.tenant_name) as tenant_name,
        coalesce(dr.game_type, dp.game_type) as game_type,
        coalesce(dr.currency, dp.currency) as currency,
        coalesce(dr.gross_revenue, 0) as gross_revenue,
        coalesce(dp.gross_prizes, 0) as gross_prizes,
        coalesce(dr.gross_revenue, 0) - coalesce(dp.gross_prizes, 0) as gross_profit
    from daily_revenue as dr
    full outer join daily_prizes as dp
        on dr.date_day = dp.date_day
        and dr.client_id = dp.client_id
        and dr.tenant_id = dp.tenant_id
        and dr.currency = dp.currency

)

select
    date_day,
    client_id,
    tenant_id,
    tenant_name,
    game_type,
    currency,
    gross_revenue,
    gross_prizes,
    gross_profit
from joined
