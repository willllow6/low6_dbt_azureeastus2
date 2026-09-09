{{
    config(
        materialized='incremental',
        unique_key='selection_id',
        incremental_strategy='merge'
    )
}}

with

selections as (

    select *
    from {{ ref('stg_bet365_overunder__picks') }}

    {% if is_incremental() %}
    where picked_at_et >= (
        select dateadd(day, -{{ var('bet365_overunder_incremental_lookback_days') }}, max(picked_at_et))
        from {{ this }}
    )
    {% endif %}

)

select

    pick_id as selection_id,
    entry_id,
    prop_id,

    client_id,
    tenant_id,
    game_type,

    picked_option,
    sport_name,
    player_name,
    player_position,
    team_abbr,
    opponent_abbr,
    fixture_name,
    market_name,
    market_handicap,
    odds_fractional,
    odds_decimal,
    odds_american,
    is_correct,

    picked_at,
    picked_at_et

from selections
