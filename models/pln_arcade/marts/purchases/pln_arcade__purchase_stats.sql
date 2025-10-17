with

purchases as (

    select *
    from {{ ref('pln_arcade__purchases') }}

),

purchase_stats as (

    select
        app_store,
        item_name,
        purchased_date,
        purchased_date_et,
        count(*) as purchases,
        sum(case when user_purchase_number = 1 then 1 else 0 end) as first_purchases,
        sum(price) as gross_revenue,
        sum(coin_amount) as coins_purchased
    from purchases
    group by 1,2,3,4

)

select * from purchase_stats