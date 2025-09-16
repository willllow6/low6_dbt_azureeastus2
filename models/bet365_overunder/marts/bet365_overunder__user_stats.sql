with

users as (

    select *
    from {{ ref('bet365_overunder__users') }}

),

user_stats as (

    select
        country,
        state_province,
        segment_group,
        registration_date,
        registration_date_et,
        count(*) as registrations,
        sum(case when has_logged_in_since_launch then 1 else 0 end) as users_logged_in_since_launch
    from users
    group by 1,2,3,4,5

)

select * from user_stats