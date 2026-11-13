with

selections as (

    select *
    from {{ ref('stg_bet365_pickem__selections') }}

),

entries as (

    select *
    from {{ ref('stg_bet365_pickem__entries') }}
    where is_valid

)

select
    selections.selection_id,
    selections.entry_id,
    entries.user_id,
    selections.contest_id,
    selections.contest_fixture_id,
    selections.question_id,
    selections.option_id,

    entries.client_id,
    entries.tenant_id,
    entries.tenant_name,
    selections.game_type,

    entries.country_code,
    entries.state_code,
    selections.pick_input,
    selections.price_fractional,
    selections.price_american,

    selections.price_decimal,
    selections.potential_returns,

    selections.selected_at,
    convert_timezone('UTC', '{{ var("local_timezone") }}', selections.selected_at) as selected_at_et,
    selections.created_at

from selections
inner join entries
    on selections.entry_id = entries.entry_id
