with

entries as (

    select *
    from {{ ref('int_olybet_casino__entries') }}

)

select
    entry_id,
    user_id,
    contest_id,
    client_id,
    tenant_id,
    tenant_name,
    game_type,
    contest_name,
    contest_status,
    game_name,
    tickets_played,
    tickets_won,
    is_winner,
    entry_status,
    is_active,
    user_entry_number,
    contest_start_date,
    cast(convert_timezone('UTC', '{{ var("olybet_casino_local_timezone") }}', contest_starts_at) as date) as contest_start_date_et,
    contest_starts_at,
    convert_timezone('UTC', '{{ var("olybet_casino_local_timezone") }}', contest_starts_at)::timestamp_ntz as contest_starts_at_et,
    entry_date,
    cast(convert_timezone('UTC', '{{ var("olybet_casino_local_timezone") }}', entered_at) as date) as entry_date_et,
    entered_at,
    convert_timezone('UTC', '{{ var("olybet_casino_local_timezone") }}', entered_at)::timestamp_ntz as entered_at_et,
    updated_at
from entries
