with

tickets as (

    select *
    from {{ ref('int_olybet_casino__tickets') }}

)

select
    ticket_id,
    entry_id,
    user_id,
    contest_id,
    contest_name,
    client_id,
    tenant_id,
    tenant_name,
    game_type,
    competition_prize_id,
    prize_tier,
    win_chance,
    prize_id,
    prize_name,
    prize_type,
    prize_value,
    ticket_status,
    is_played,
    played_date,
    cast(convert_timezone('UTC', '{{ var("olybet_casino_local_timezone") }}', played_at) as date) as played_date_et,
    played_at,
    convert_timezone('UTC', '{{ var("olybet_casino_local_timezone") }}', played_at)::timestamp_ntz as played_at_et,
    created_at,
    cast(convert_timezone('UTC', '{{ var("olybet_casino_local_timezone") }}', created_at) as date) as created_date_et,
    convert_timezone('UTC', '{{ var("olybet_casino_local_timezone") }}', created_at)::timestamp_ntz as created_at_et,
    updated_at
from tickets
