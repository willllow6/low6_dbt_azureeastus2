{{ config(
    materialized='view',
    secure=true,
    post_hook="{{ share_view('analytics_oilers_picks', 'shared_oilers_picks__entries', 'oilers_share') }}"
) }}

-- Client contract: column names, types and order are fixed (see
-- _schema_oilers_picks_sharing.yml). Ids are cast back to the NUMBER types the
-- share has always exposed. Entries in DRAFT contests have never been shared.

with

entries as (

    select *
    from {{ ref('fct_oilers_picks__entries') }}

),

users as (

    select *
    from {{ ref('dim_oilers_picks__users') }}

),

contests as (

    select *
    from {{ ref('dim_oilers_picks__contests') }}

)

select
    entries.entry_id,
    cast(entries.user_id as number(38, 0)) as user_id,
    cast(entries.contest_id as number(38, 0)) as pickem_id,
    cast(users.service_user_id as number(38, 0)) as service_user_id,
    cast(entries.entry_score_id as number(38, 0)) as entry_score_id,

    users.email,
    users.username as low6_username,
    users.first_name,
    users.last_name,
    users.postal_code,
    users.mobile_number,
    users.marketing_consent as has_consented_marketing,
    users.has_placed_sports_bet,
    cast(users.registered_at as date) as user_created_date,
    users.registration_date_et as user_created_date_et,

    contests.contest_name as pickem_title,
    contests.contest_status as pickem_status,
    contests.prize_text as pickem_prize,
    cast(contests.starts_at as date) as pickem_start_date,
    contests.contest_start_date_et as pickem_start_date_et,
    contests.starts_at as pickem_starts_at,

    cast(entries.entry_points as number(38, 0)) as entry_points,
    cast(entries.tiebreak_prediction as number(38, 0)) as tiebreak_prediction,
    cast(contests.tiebreak_outcome as number(38, 0)) as tiebreak_outcome,
    cast(entries.tiebreak_error as number(38, 0)) as tiebreak_error,
    cast(entries.user_entry_number as number(18, 0)) as user_entry_number,
    case
        when entries.user_entry_number = 1
            then 'First Entry'
        else 'Subsequent Entry'
    end as user_entry_type,

    cast(entries.entered_at as date) as entry_date,
    entries.entry_date_et,
    cast(day(entries.entry_date_et) as number(2, 0)) as entry_day_et,
    cast(hour(entries.entered_at_et) as number(2, 0)) as entry_hour_et,
    entries.entered_at,
    entries.entered_at_et

from entries
inner join users
    on entries.user_id = users.user_id
inner join contests
    on entries.contest_id = contests.contest_id
where contests.contest_status != 'DRAFT'
