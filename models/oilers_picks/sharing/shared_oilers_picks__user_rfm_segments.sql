{{ config(
    materialized='view',
    secure=true,
    post_hook="{{ share_view('analytics_oilers_picks', 'shared_oilers_picks__user_rfm_segments', 'oilers_share') }}"
) }}

-- Client contract: column names, types and order are fixed (see
-- _schema_oilers_picks_sharing.yml). Segments are measured against the time
-- the client queries the view. Entries in DRAFT contests have never counted.

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

),

scoped_entries as (

    select
        entries.user_id,
        entries.entry_date_et
    from entries
    inner join users
        on entries.user_id = users.user_id
    inner join contests
        on entries.contest_id = contests.contest_id
    where contests.contest_status != 'DRAFT'

),

{{ user_rfm_segments_ctes('scoped_entries') }}

select
    cast(user_id as number(38, 0)) as user_id,
    last_entry_date,
    cast(entries as number(18, 0)) as entries,
    cast(days_since_last_entry as number(9, 0)) as days_since_last_entry,
    frequency,
    recency,
    segment

from user_rfm_segments
