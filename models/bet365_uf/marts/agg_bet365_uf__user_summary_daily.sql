with

users as (
    select *
    from {{ ref('dim_bet365_uf__users') }}
    where is_tester = false
),

agg_users as (

    select
        user_state,
        user_age_band,
        user_generation,
        has_consented_marketing,
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', registered_at) as date) as registered_date,
        time_to_first_purchase_band,
        gross_revenue_band,
        count(*) as registrations,
        sum(case when is_playable then 1 else 0 end) as playable_users,
        sum(case when is_returning_user then 1 else 0 end) as returning_users,
        sum(case when is_paying_user then 1 else 0 end) as paying_users,
        sum(case when has_completed_profile then 1 else 0 end) as completed_profiles,
        sum(purchases) as purchases,
        sum(gross_revenue) as gross_revenue
    from users
    group by 1, 2, 3, 4, 5, 6, 7

)

select * from agg_users
