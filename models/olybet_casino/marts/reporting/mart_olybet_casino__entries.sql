with

entries as (

    select *
    from {{ ref('fct_olybet_casino__entries') }}

),

users as (

    select
        user_id,
        registered_at_et
    from {{ ref('dim_olybet_casino__users') }}

)

select
    entries.entry_id,
    entries.user_id,
    users.registered_at_et as user_registered_at_et,
    entries.contest_id,
    entries.contest_name,
    entries.game_type,
    entries.tenant_name,
    entries.game_name,
    entries.contest_status,
    entries.tickets_played,
    entries.tickets_won,
    entries.is_winner,
    entries.entry_status,
    entries.user_entry_number,
    entries.contest_start_date,
    entries.contest_start_date_et,
    entries.contest_starts_at,
    entries.contest_starts_at_et,
    entries.entry_date,
    entries.entry_date_et,
    entries.entered_at,
    entries.entered_at_et,
    entries.updated_at
from entries
inner join users
    on entries.user_id = users.user_id
