with

users as (

    select *
    from {{ ref('dim_bet365_overunder__users') }}

),

registration_summary as (

    select
        registration_date_et as date_day,
        client_id,
        tenant_id,
        country,
        state_province,
        segment_group,
        count(*) as registrations,
        sum(case when has_logged_in_since_launch then 1 else 0 end) as users_logged_in_since_launch
    from users
    group by 1,2,3,4,5,6

)

select * from registration_summary
