with

entries as (

    select *
    from {{ ref('fct_opap_spintowin__entries') }}

),

users as (

    select
        user_id,
        sso_user_id,
        user_state
    from {{ ref('dim_opap_spintowin__users') }}

)

select
    entries.entry_id,
    entries.user_id,
    users.sso_user_id,
    users.user_state,
    entries.contest_id,
    entries.contest_title,
    entries.game_type,
    entries.tenant_name,
    entries.contest_status,
    entries.contest_start_date,
    entries.contest_start_date_et,
    entries.contest_starts_at,
    entries.contest_starts_at_et,
    entries.points,
    entries.prize_type,
    entries.prize_amount,
    entries.tiebreaker_prediction,
    entries.tiebreaker_outcome,
    entries.tiebreaker_error,
    entries.entry_status,
    entries.user_entry_number,
    entries.entry_date,
    entries.entry_date_et,
    entries.entered_at,
    entries.entered_at_et,
    entries.updated_at
from entries
inner join users
    on entries.user_id = users.user_id
