with

app_store_purchases as (

    select *
    from {{ ref('mart_bet365_uf__app_store_purchases') }}

),

user_app_store_purchases as (

    select
        user_id,
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', purchased_at) as date) as purchased_date,
        count(*) as purchase_count,
        sum(purchase_price) as purchase_amount
    from app_store_purchases
    group by 1, 2

)

select * from user_app_store_purchases
