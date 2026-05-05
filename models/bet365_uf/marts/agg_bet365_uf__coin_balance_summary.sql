with

user_coin_balances as (

    select *
    from {{ ref('mart_bet365_uf__user_coin_balances') }}
    where is_tester = false

),

balance_band_stats as (

    select
        coin_balance_band,
        count(*) as users,
        sum(coin_balance) as total_coin_balance
    from user_coin_balances
    group by 1

)

select * from balance_band_stats
