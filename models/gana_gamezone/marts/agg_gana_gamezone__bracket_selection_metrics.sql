with

selections as (

    select * from {{ ref('mart_gana_gamezone__bracket_selections') }}

),

aggregated as (

    select
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        contest_id,
        match_id,
        round,
        bracket_position,
        competing_teams,
        selected_country_id,
        selected_country_name,
        selected_country_code,
        count(*)                                                                            as total_picks,
        sum(case when is_correct then 1 else 0 end)                                        as correct_picks,
        div0(
            sum(case when is_correct then 1 else 0 end),
            sum(case when is_correct is not null then 1 else 0 end)
        )                                                                                   as accuracy_rate
    from selections
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12

)

select * from aggregated
