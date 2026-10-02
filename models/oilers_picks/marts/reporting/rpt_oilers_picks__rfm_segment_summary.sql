with

entries as (

    select *
    from {{ ref('fct_oilers_picks__entries') }}

),

contests as (

    select *
    from {{ ref('dim_oilers_picks__contests') }}

),

-- Entries in DRAFT contests are excluded, matching the Tableau dashboard and
-- shared_oilers_picks__user_rfm_segments. The platform resets every contest to
-- DRAFT at season rollover, so this only covers the current season's
-- published contests.
scoped_entries as (

    select
        entries.user_id,
        entries.entry_date_et
    from entries
    inner join contests
        on entries.contest_id = contests.contest_id
    where contests.contest_status != 'DRAFT'

),

{{ user_rfm_segments_ctes('scoped_entries') }}

select
    recency,
    frequency,
    segment,

    count(user_id) as users,
    median(entries) as entries_median,
    median(days_since_last_entry) as days_since_last_entry_median

from user_rfm_segments
group by 1, 2, 3
