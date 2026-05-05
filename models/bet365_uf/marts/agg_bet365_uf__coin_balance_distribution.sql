with

user_coin_balances as (

    select *
    from {{ ref('mart_bet365_uf__user_coin_balances') }}
    where is_tester = false

),

distribution_stats as (

    select
        is_paying_user,
        count(*) as users,
        percentile_cont(0) within group (order by coin_balance) as coin_balance_p0,
        percentile_cont(0.1) within group (order by coin_balance) as coin_balance_p10,
        percentile_cont(0.2) within group (order by coin_balance) as coin_balance_p20,
        percentile_cont(0.3) within group (order by coin_balance) as coin_balance_p30,
        percentile_cont(0.4) within group (order by coin_balance) as coin_balance_p40,
        percentile_cont(0.5) within group (order by coin_balance) as coin_balance_median,
        percentile_cont(0.6) within group (order by coin_balance) as coin_balance_p60,
        percentile_cont(0.7) within group (order by coin_balance) as coin_balance_p70,
        percentile_cont(0.8) within group (order by coin_balance) as coin_balance_p80,
        percentile_cont(0.9) within group (order by coin_balance) as coin_balance_p90,
        percentile_cont(0.95) within group (order by coin_balance) as coin_balance_p95,
        percentile_cont(0.99) within group (order by coin_balance) as coin_balance_p99,
        percentile_cont(1) within group (order by coin_balance) as coin_balance_max,
        avg(coin_balance) as coin_balance_average
    from user_coin_balances
    group by 1

)

select * from distribution_stats
