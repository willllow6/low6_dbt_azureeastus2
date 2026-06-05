with

entries as (

    select *
    from {{ ref('int_opap_spintowin__entries') }}

)

select
    entry_id,
    user_id,
    contest_id,
    client_id,
    tenant_id,
    tenant_name,
    game_type,
    contest_title,
    contest_status,
    contest_start_date,
    cast(convert_timezone('UTC', '{{ var("opap_spintowin_local_timezone") }}', contest_starts_at) as date) as contest_start_date_et,
    contest_starts_at,
    convert_timezone('UTC', '{{ var("opap_spintowin_local_timezone") }}', contest_starts_at)::timestamp_ntz as contest_starts_at_et,
    points,
    prize_tier_id,
    prize_type,
    prize_amount,
    tiebreaker_prediction,
    tiebreaker_outcome,
    tiebreaker_error,
    entry_status,
    is_active,
    user_entry_number,
    entry_date,
    cast(convert_timezone('UTC', '{{ var("opap_spintowin_local_timezone") }}', entered_at) as date) as entry_date_et,
    entered_at,
    convert_timezone('UTC', '{{ var("opap_spintowin_local_timezone") }}', entered_at)::timestamp_ntz as entered_at_et,
    settled_at,
    updated_at
from entries
