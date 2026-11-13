with

entries as (

    select *
    from {{ ref('stg_bet365_pickem__entries') }}
    where is_valid

),

entry_selections as (

    select *
    from {{ ref('int_bet365_pickem__selections_aggregated_to_entries') }}

),

standings as (

    select *
    from {{ ref('stg_bet365_pickem__standings') }}

)

select
    entries.entry_id,
    entries.user_id,
    entries.contest_id,
    entries.boosted_contest_fixture_id,

    entries.client_id,
    entries.tenant_id,
    entries.tenant_name,
    entries.game_type,

    entries.country_code,
    entries.state_code,
    entries.currency,
    entries.entry_status,

    coalesce(entry_selections.selections, 0) as selections,
    entry_selections.avg_price_decimal,
    entry_selections.potential_returns,
    standings.pick_points,
    standings.bonus_points,
    standings.score,
    standings.entry_rank,
    row_number() over (
        partition by entries.user_id
        order by entries.entered_at, entries.entry_id
    ) as user_entry_number,

    cast(
        convert_timezone('UTC', '{{ var("local_timezone") }}', entries.entered_at) as date
    ) as entry_date_et,

    entries.created_at,
    entries.entered_at,
    convert_timezone('UTC', '{{ var("local_timezone") }}', entries.entered_at) as entered_at_et,
    entries.counted_at,
    entry_selections.last_selected_at,
    standings.updated_at

from entries
left join entry_selections
    on entries.entry_id = entry_selections.entry_id
left join standings
    on entries.entry_id = standings.entry_id
