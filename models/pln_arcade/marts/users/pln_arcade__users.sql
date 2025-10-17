with 

users as (

    select *
    from {{ ref('stg_pln_arcade__users') }}

),

coin_transactions as (

    select *
    from {{ ref('stg_pln_arcade__coin_transactions') }}

),

purchases as (

    select *
    from {{ ref('stg_pln_arcade__store_purchases') }}

),

coin_balances as (

    select
        user_id,
        sum(coin_amount) as coin_balance
    from coin_transactions
    group by user_id

),

joined as (

    select
        users.user_id,
        users.username,
        users.email,
        users.date_of_birth,
        users.generation,
        users.age,
        users.age_band,
        users.is_enabled,
        users.has_consented_pln_marketing,
        coalesce(coin_balances.coin_balance,0) as coin_balance,
        users.user_created_date,
        users.user_created_date_et,
        users.user_created_at,
        users.user_created_at_et
    from users
    left join coin_balances
        on users.user_id = coin_balances.user_id
    
)

select * from joined