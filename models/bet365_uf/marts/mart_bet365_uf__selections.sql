with

selections as (
    select * from {{ ref('fct_bet365_uf__selections') }}
),

scoreables as (
    select * from {{ ref('dim_bet365_uf__scoreables') }}
),

entries as (

    select
        entry_id,
        contest_id,
        tenant_id,
        tenant_name,
        competition_name,
        contest_name,
        game_week_name,
        game_week_start_date,
        contest_start_date,
        is_tester
    from {{ ref('mart_bet365_uf__entries') }}

),

joined as (

    select
        selections.selection_id,
        selections.entry_id,
        selections.tenant_id,
        selections.user_id,
        selections.card_id,
        selections.scoreable_id,
        selections.client_id,
        selections.game_type,
        selections.selection_position,
        selections.selection_modifier,
        selections.selection_score,
        selections.selection_score_breakdown,
        selections.match_status,
        selections.selected_date,
        selections.selected_at,
        selections.updated_at,
        scoreables.scoreable_name,
        scoreables.scoreable_rating,
        scoreables.scoreable_position,
        scoreables.scoreable_team_name,
        entries.contest_id,
        entries.tenant_name,
        entries.competition_name,
        entries.contest_name,
        entries.game_week_name,
        entries.game_week_start_date,
        entries.contest_start_date,
        entries.is_tester
    from selections
    left join scoreables
        on selections.scoreable_id = scoreables.scoreable_id
    inner join entries
        on selections.entry_id = entries.entry_id

)

select * from joined
