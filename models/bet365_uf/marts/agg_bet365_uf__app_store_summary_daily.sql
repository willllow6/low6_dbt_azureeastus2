with

purchases as (

    select *
    from {{ ref('mart_bet365_uf__app_store_purchases') }}
    where is_tester = false

),

agg_purchases as (

    select
        app_store_name,
        item_name,
        item_description,
        coins_amount,
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', purchased_at) as date) as purchased_date,
        count(*) as purchases,
        sum(case when user_purchase_number = 1 then 1 else 0 end) as first_purchases,
        sum(purchase_price) as gross_revenue
    from purchases
    group by 1, 2, 3, 4, 5

)

select * from agg_purchases
