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
        count(*) as users,
        sum(coin_balance) as total_coins
    from users
    group by 1,2,3,4,5,6,7

)

select * from user_stats