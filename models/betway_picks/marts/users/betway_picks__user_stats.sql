with

users as (

     select * from {{ ref('betway_picks__users') }}

),

entrants as (

    select distinct user_id
    from {{ ref('betway_picks__entries') }}

),

user_stats as (

    select
        users.registration_date,
        users.registration_date_et,
        users.region,
        entrants.user_id is not null as has_first_entry,
        count(*) as registrations
    from users
    left join entrants
        on users.user_id = entrants.user_id
    group by 1,2,3,4

)

select * from user_stats
