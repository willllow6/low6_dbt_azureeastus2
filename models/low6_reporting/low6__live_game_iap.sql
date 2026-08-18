with

b365fan_iap as (

    select
        'b365fan' as app_id,
        sum(purchase_price) as gross_revenue,
        sum(case when cast(purchased_at as date) = current_date() - 1 then purchase_price else null end) as yesterday_gross_revenue,
        sum(case when cast(purchased_at as date) >= current_date() - 8 and cast(purchased_at as date) < current_date() then purchase_price else null end) as last_7_days_gross_revenue,
        sum(case when cast(purchased_at as date) >= current_date() - 29 and cast(purchased_at as date) < current_date() then purchase_price else null end) as last_28_days_gross_revenue
    from {{ ref('mart_bet365_uf__app_store_purchases') }} as p 
    inner join {{ ref('stg_bet365_uf__users') }} as u 
        on p.user_id = u.user_id 
    where u.is_tester = false
        
)

select * from b365fan_iap