with

transactions as (

    select *
    from {{ ref('stg_pln_arcade__coin_transactions') }}

),

users as (

    select *
    from {{ ref('stg_pln_arcade__users') }}

),

joined as (

    select

        transactions.coin_transaction_id,
        transactions.user_id,

        users.username,

        transactions.transaction_reference,
        transactions.transaction_direction,
        transactions.transaction_type,
        transactions.coin_amount,

        transactions.created_date,
        transactions.created_date_et,
        transactions.created_at,
        transactions.created_at_et

    from transactions
    inner join users 
        on transactions.user_id = users.user_id

)

select * from joined