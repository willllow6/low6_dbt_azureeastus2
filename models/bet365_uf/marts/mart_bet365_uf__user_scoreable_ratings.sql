with

scoreables as (
    select scoreable_id, scoreable_rating, count(*) as total_available_scoreables
    from {{ ref('stg_bet365_uf__scoreables') }}
    group by 1, 2
),

total_by_rating as (

    select
        scoreable_rating,
        count(*) as total_available_scoreables
    from {{ ref('stg_bet365_uf__scoreables') }}
    group by 1

),

user_cards as (
    select * from {{ ref('mart_bet365_uf__user_cards') }}
),

users as (
    select user_id, is_paying_user, registered_date, username, is_tester
    from {{ ref('dim_bet365_uf__users') }}
),

user_owned_scoreables as (

    select
        user_id,
        scoreable_rating,
        count(distinct scoreable_id) as scoreables_owned
    from user_cards
    group by 1, 2

),

joined as (

    select
        user_owned_scoreables.user_id,
        user_owned_scoreables.scoreable_rating,
        total_by_rating.total_available_scoreables,
        user_owned_scoreables.scoreables_owned,
        div0(user_owned_scoreables.scoreables_owned, total_by_rating.total_available_scoreables) as ownership_pct,
        case
            when div0(user_owned_scoreables.scoreables_owned, total_by_rating.total_available_scoreables) < 0.1 then '0%-9.99%'
            when div0(user_owned_scoreables.scoreables_owned, total_by_rating.total_available_scoreables) < 0.2 then '10%-19.99%'
            when div0(user_owned_scoreables.scoreables_owned, total_by_rating.total_available_scoreables) < 0.3 then '20%-29.99%'
            when div0(user_owned_scoreables.scoreables_owned, total_by_rating.total_available_scoreables) < 0.4 then '30%-39.99%'
            when div0(user_owned_scoreables.scoreables_owned, total_by_rating.total_available_scoreables) < 0.5 then '40%-49.99%'
            when div0(user_owned_scoreables.scoreables_owned, total_by_rating.total_available_scoreables) < 0.6 then '50%-59.99%'
            when div0(user_owned_scoreables.scoreables_owned, total_by_rating.total_available_scoreables) < 0.7 then '60%-69.99%'
            when div0(user_owned_scoreables.scoreables_owned, total_by_rating.total_available_scoreables) < 0.8 then '70%-79.99%'
            when div0(user_owned_scoreables.scoreables_owned, total_by_rating.total_available_scoreables) < 0.9 then '80%-89.99%'
            when div0(user_owned_scoreables.scoreables_owned, total_by_rating.total_available_scoreables) < 1 then '90%-99.99%'
            else '100%'
        end as ownership_band,
        users.is_paying_user,
        users.registered_date,
        users.username,
        users.is_tester
    from user_owned_scoreables
    left join total_by_rating
        on user_owned_scoreables.scoreable_rating = total_by_rating.scoreable_rating
    left join users
        on user_owned_scoreables.user_id = users.user_id

)

select * from joined
