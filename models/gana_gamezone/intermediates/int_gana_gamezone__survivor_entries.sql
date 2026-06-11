with

survivor_selections as (

    select * from {{ ref('stg_gana_gamezone__survivor_selections') }}

),

survivor_scores as (

    select * from {{ ref('stg_gana_gamezone__survivor_scores') }}

),

joined as (

    select
        sp.user_id || '-' || sp.contest_id  as entry_id,
        ss.score_id,
        sp.user_id,
        sp.client_id,
        sp.tenant_id,
        sp.tenant_name,
        sp.game_type,
        sp.contest_id,
        sp.selection_id,
        sp.selected_country_id,
        ss.is_correct,
        ss.current_streak,
        ss.best_streak,
        sp.selected_at,
        sp.selected_at                      as created_at,
        sp.updated_at
    from survivor_selections as sp
    left join survivor_scores as ss
        on sp.selection_id = ss.selection_id

)

select * from joined
