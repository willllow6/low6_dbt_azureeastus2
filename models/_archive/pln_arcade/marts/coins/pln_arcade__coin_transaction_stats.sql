with

transactions as (

    select *
    from {{ ref('pln_arcade__coin_transactions') }}

),

transaction_stats as (

    select
        transaction_direction,
        transaction_type,
        created_date,
        created_date_et,
        count(*) as transactions,
        sum(abs(coin_amount)) as coins,
        sum(coin_amount) as coins_direction
    from transactions
    group by 1,2,3,4

)

select * from transaction_stats