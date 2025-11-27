with

users as (
  
     select * from {{ ref('bet99_picks__users') }} 

),

user_stats as (

    select
        registration_date,
        registration_date_et,
        region,
        count(*) as registrations
    from users
    group by 1,2,3

)

select * from user_stats