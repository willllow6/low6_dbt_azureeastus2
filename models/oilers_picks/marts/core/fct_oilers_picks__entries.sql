with

entries as (

    select *
    from {{ ref('int_oilers_picks__selections_aggregated_to_entries') }}

),

entry_scores as (

    select *
    from {{ ref('stg_oilers_picks__entry_scores') }}

),

contests as (

    select *
    from {{ ref('dim_oilers_picks__contests') }}

)

select
    entries.entry_id,
    entries.user_id,
    entries.contest_id,
    entry_scores.entry_score_id,

    entries.client_id,
    entries.tenant_id,
    entries.tenant_name,
    entries.game_type,

    entries.selections,
    entry_scores.entry_points,
    entries.tiebreak_prediction,
    round(abs(entries.tiebreak_prediction - contests.tiebreak_outcome)) as tiebreak_error,
    entries.user_entry_number,

    cast(
        convert_timezone('UTC', '{{ var("local_timezone") }}', entries.entered_at) as date
    ) as entry_date_et,

    entries.entered_at,
    convert_timezone('UTC', '{{ var("local_timezone") }}', entries.entered_at) as entered_at_et

from entries
left join entry_scores
    on entries.user_id = entry_scores.user_id
    and entries.contest_id = entry_scores.contest_id
left join contests
    on entries.contest_id = contests.contest_id
