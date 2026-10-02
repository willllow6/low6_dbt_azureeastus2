-- Compatibility view for the Oilers Picks Tableau dashboard. Repoint the
-- dashboard to rpt_oilers_picks__entry_summary_hourly, then delete this model.

with

entry_summary as (

    select *
    from {{ ref('rpt_oilers_picks__entry_summary_hourly') }}

)

select
    marketing_consent as has_consented_marketing,
    has_placed_sports_bet,

    contest_name as pickem_title,
    contest_status as pickem_status,
    contest_start_date_et as pickem_start_date_et,

    entry_type as user_entry_type,
    entry_date_et,
    day(entry_date_et) as entry_day_et,
    entry_hour_et,

    sum(entries) as entries,
    sum(first_entries) as first_entries

from entry_summary
where contest_status != 'DRAFT'
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
