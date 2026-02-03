with

users as (

    select *
    from {{ ref('fanstake_rivals__users') }}

),

user_stats as (

    select
        registration_date_et,
        team_name,
        team_sport,
        team_league,
        count(*) as registrations
    from users
    where user_role = 'user'
    group by 1,2,3,4

)

select * from user_stats