with

selections as (
    select *
    from {{ ref('mart_bet365_uf__selections') }}
    where is_tester = false
),

agg_selections as (

    select
        tenant_name,
        competition_name,
        game_week_name,
        game_week_start_date,
        contest_name,
        contest_start_date,
        scoreable_id,
        scoreable_name,
        scoreable_position,
        scoreable_team_name,
        count(scoreable_id) as selections
    from selections
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

),

with_previous_week as (

    select
        *,
        lag(selections, 1) over (
            partition by scoreable_id, competition_name
            order by scoreable_id, game_week_start_date
        ) as previous_game_week_selections
    from agg_selections

),

final as (

    select
        tenant_name,
        competition_name,
        game_week_name,
        game_week_start_date,
        contest_name,
        contest_start_date,
        scoreable_id,
        scoreable_name,
        scoreable_position,
        scoreable_team_name,
        selections,
        coalesce(previous_game_week_selections, 0) as previous_game_week_selections,
        selections - coalesce(previous_game_week_selections, 0) as selections_change_from_previous_game_week,
        div0(
            selections - coalesce(previous_game_week_selections, 0),
            coalesce(previous_game_week_selections, 0)
        ) * 100 as selections_percent_change_from_previous_game_week
    from with_previous_week

)

select * from final
