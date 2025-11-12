with

users as (

    select *
    from {{ ref('pln_arcade__users') }}

),

user_stats as (

    select
        user_created_date,
        user_created_date_et,
        generation,
        age,
        age_band,
        is_enabled,
        has_consented_pln_marketing,
        gross_revenue_band,
        count(*) as users,
        sum(coin_balance) as total_coins,
        sum(total_gross_revenue) as total_gross_revenue,
        sum(case when total_gross_revenue > 0 then 1 else 0 end) as paying_users
    from users
    group by 1,2,3,4,5,6,7,8

)

select * from user_stats