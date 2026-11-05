with

entries as (

    select *
    from {{ ref('stg_du_trivia__entries') }}

),

entry_selections as (

    select *
    from {{ ref('int_du_trivia__selections_aggregated_to_entries') }}

)

select
    entries.entry_id,
    entries.user_id,
    entries.contest_id,

    entries.client_id,
    entries.tenant_id,
    entries.tenant_name,
    entries.game_type,

    entries.entry_points,
    coalesce(entry_selections.selections, 0) as selections,
    coalesce(entry_selections.correct_selections, 0) as correct_selections,
    entry_selections.elapsed_seconds,
    row_number() over (
        partition by entries.user_id
        order by entries.entered_at, entries.contest_id
    ) as user_entry_number,

    entries.is_completed,
    entries.has_max_points,

    cast(
        convert_timezone('UTC', '{{ var("du_trivia_local_timezone") }}', entries.entered_at) as date
    ) as entry_date_gst,

    entries.entered_at,
    convert_timezone('UTC', '{{ var("du_trivia_local_timezone") }}', entries.entered_at) as entered_at_gst,
    entries.completed_at,
    entries.updated_at

from entries
left join entry_selections
    on entries.entry_id = entry_selections.entry_id
