with

competitions as (

    select *
    from {{ ref('stg_olybet_casino__competitions') }}

),

games as (

    select
        game_id,
        game_name,
        prize_scheme
    from {{ ref('stg_olybet_casino__games') }}

)

select
    c.contest_id,
    c.client_id,
    c.tenant_id,
    c.tenant_name,
    c.game_type,
    c.contest_name,
    c.contest_description,
    c.contest_status,
    c.contest_image_url,
    g.game_name,
    g.prize_scheme,
    c.max_tickets,
    c.ticket_min,
    c.ticket_default,
    c.ticket_max,
    c.user_max_entries,
    c.tickets_issued,
    c.is_active,
    cast(null as number) as entry_fee,
    cast(null as number) as prize_pool,
    c.contest_start_date,
    cast(convert_timezone('UTC', '{{ var("olybet_casino_local_timezone") }}', c.contest_starts_at) as date) as contest_start_date_et,
    c.contest_starts_at,
    convert_timezone('UTC', '{{ var("olybet_casino_local_timezone") }}', c.contest_starts_at)::timestamp_ntz as contest_starts_at_et,
    c.contest_ends_at,
    convert_timezone('UTC', '{{ var("olybet_casino_local_timezone") }}', c.contest_ends_at)::timestamp_ntz as contest_ends_at_et,
    c.created_at,
    c.updated_at
from competitions c
left join games g
    on c.game_id = g.game_id
