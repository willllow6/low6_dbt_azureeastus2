with

coin_transactions as (
    select * from {{ ref('mart_bet365_uf__coin_transactions') }}
),

user_balance as (

    select
        user_id,
        sum(coin_amount) as coin_balance
    from coin_transactions
    group by 1

),

users as (
    select * from {{ ref('dim_bet365_uf__users') }}
),

joined as (

    select
        users.user_id,
        users.client_id,
        users.email,
        users.username,
        users.first_name,
        users.last_name,
        users.is_playable,
        users.is_paying_user,
        users.is_tester,
        coalesce(user_balance.coin_balance, 0) as coin_balance,
        case
            when coalesce(user_balance.coin_balance, 0) = 0 then '0'
            when user_balance.coin_balance < 101 then '1-100'
            when user_balance.coin_balance < 201 then '101-200'
            when user_balance.coin_balance < 301 then '201-300'
            when user_balance.coin_balance < 401 then '301-400'
            when user_balance.coin_balance < 501 then '401-500'
            when user_balance.coin_balance < 1001 then '501-1000'
            when user_balance.coin_balance < 2001 then '1001-2000'
            when user_balance.coin_balance < 5001 then '2001-5000'
            when user_balance.coin_balance < 10001 then '5001-10000'
            else '10000+'
        end as coin_balance_band
    from users
    left join user_balance
        on users.user_id = user_balance.user_id

)

select * from joined
