{{
    config(
        materialized='incremental',
        unique_key='prop_summary_id',
        incremental_strategy='merge'
    )
}}

with

selections as (

    select *
    from {{ ref('fct_bet365_overunder__selections') }}

    {% if is_incremental() %}
    where picked_at_et >= (
        select dateadd(day, -{{ var('bet365_overunder_incremental_lookback_days') }}, max(contest_date_et))
        from {{ this }}
    )
    {% endif %}

),

entries as (

    select
        entry_id,
        country,
        contest_date_et,
        entry_date_et
    from {{ ref('fct_bet365_overunder__entries') }}

),

joined as (

    select
        entries.contest_date_et,
        entries.entry_date_et,
        selections.client_id,
        selections.tenant_id,
        selections.sport_name,
        selections.fixture_name,
        selections.player_name,
        selections.market_name,
        selections.market_handicap,
        selections.picked_option,
        entries.country
    from selections
    inner join entries
        on selections.entry_id = entries.entry_id

),

prop_summary as (

    select
        {{ dbt_utils.generate_surrogate_key(['contest_date_et', 'entry_date_et', 'sport_name', 'fixture_name', 'player_name', 'market_name', 'market_handicap', 'picked_option', 'country']) }} as prop_summary_id,
        contest_date_et,
        entry_date_et,
        client_id,
        tenant_id,
        sport_name,
        fixture_name,
        player_name,
        market_name,
        market_handicap,
        picked_option,
        country,
        count(*) as picks
    from joined
    group by 1,2,3,4,5,6,7,8,9,10,11,12

)

select * from prop_summary
