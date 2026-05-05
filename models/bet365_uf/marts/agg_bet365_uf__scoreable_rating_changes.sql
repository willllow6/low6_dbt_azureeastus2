with

user_scoreable_ratings as (

    select * from {{ ref('mart_bet365_uf__user_scoreable_ratings') }}

),

rating_progression_stats as (

    select
        registered_date,
        is_paying_user,
        scoreable_rating,
        ownership_band,
        total_available_scoreables,
        count(*) as users,
        sum(scoreables_owned) as scoreables_owned,
        max(scoreables_owned) as max_scoreables_owned_by_users
    from user_scoreable_ratings
    group by 1, 2, 3, 4, 5

)

select * from rating_progression_stats
