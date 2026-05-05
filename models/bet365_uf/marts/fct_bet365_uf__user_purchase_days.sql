with

purchases as (

    select *
    from {{ ref('fct_bet365_uf__app_store_purchases') }}

),

daily_user_purchases as (

    select distinct
        user_id,
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', purchased_at) as date) as purchased_date
    from purchases

)

select * from daily_user_purchases