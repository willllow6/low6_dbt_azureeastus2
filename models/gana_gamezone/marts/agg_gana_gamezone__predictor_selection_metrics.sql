with

selections as (

    select * from {{ ref('mart_gana_gamezone__predictor_selections') }}

),

total_entrants as (

    select count(distinct user_id) as total_entrants
    from selections

),

aggregated as (

    select
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        group_id,
        group_name,
        predicted_position,
        is_predicted_to_progress,
        selected_country_id,
        selected_country_name,
        selected_country_code,
        count(*)                                            as selection_count,
        sum(predicted_position)                             as total_predicted_position
    from selections
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11

)

select
    a.client_id,
    a.tenant_id,
    a.tenant_name,
    a.game_type,
    a.group_id,
    a.group_name,
    a.predicted_position,
    a.is_predicted_to_progress,
    a.selected_country_id,
    a.selected_country_name,
    a.selected_country_code,
    a.selection_count,
    a.total_predicted_position,
    div0(a.selection_count, t.total_entrants)               as selection_pct
from aggregated as a
cross join total_entrants as t
