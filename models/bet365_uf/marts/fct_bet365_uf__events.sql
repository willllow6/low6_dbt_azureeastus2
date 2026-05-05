with

coin_transactions as (

    select
        user_id,
        coin_transaction_type as event_type,
        coin_amount,
        coin_transaction_created_at as created_at
    from {{ ref('fct_bet365_uf__coin_transactions') }}
    where is_user_triggered
),

entries as (

    select
        user_id,
        'Contest Entered' as event_type,
        0 as coin_amount,
        entered_at as created_at     
    from  {{ ref('fct_bet365_uf__entries') }}
    
),

unioned as (

    select * from coin_transactions
    
    union all
    
    select * from entries

)

select * from unioned
