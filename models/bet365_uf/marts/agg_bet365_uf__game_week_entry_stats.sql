with

entries as (
    select *
    from {{ ref('mart_bet365_uf__entries') }}
    where is_tester = false
),

game_week_stats as (

    select
        tenant_name,
        competition_name,
        game_week_name,
        game_week_description,
        game_week_start_date,
        count(*) as entries,
        count(distinct contest_id) as contests,
        count(distinct user_id) as users
    from entries
    group by 1, 2, 3, 4, 5

)

select * from game_week_stats
