with

users as (

    select *
    from {{ ref('dim_bet365_uf__users') }}
    where is_tester = false

),

conversion_cohorts as (

    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', registered_at) as date) as date_day,
        count(*) as registration_count,
        count_if(is_playable) as playable_user_count,
        count_if(has_completed_profile) as completed_profile_count,
        count_if(is_returning_user) as returning_user_count,
        count_if(has_purchased_pack) as pack_purchasers_count,
        count_if(is_paying_user) as paying_user_count,
        count_if(is_referring_user) as referring_user_count,
        sum(referral_count) as referral_count,
        sum(purchases) as purchase_count,
        sum(gross_revenue) as gross_revenue
    from users
    group by 1

)

select * from conversion_cohorts
