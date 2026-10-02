-- Compatibility view for the Oilers Picks Tableau dashboard. Repoint the
-- dashboard to rpt_oilers_picks__rfm_segment_summary, then delete this model.

with

rfm_segment_summary as (

    select *
    from {{ ref('rpt_oilers_picks__rfm_segment_summary') }}

)

select
    recency,
    frequency,
    segment,
    users as users_count,
    entries_median,
    days_since_last_entry_median

from rfm_segment_summary
