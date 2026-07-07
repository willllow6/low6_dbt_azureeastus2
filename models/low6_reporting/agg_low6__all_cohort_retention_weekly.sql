{{
    config(
        materialized='view'
    )
}}

-- No ANALYTICS_ARCHIVE tables exist yet, so this is a pure passthrough of the
-- live table. TODO: once an archive source is defined in
-- _src_low6_reporting.yml, add an archive CTE here, aliasing legacy column
-- names (first_entry_week -> cohort_week, entry_week -> activity_week,
-- weeks_from_first_entry -> weeks_since_cohort, active_users -> retained_users)
-- and union it in.

with

live as (
    select * from {{ ref('agg_low6__cohort_retention_weekly') }}
),

archive as (
    select * from {{ source('archive','game_cohort_retention_weekly') }}
)

select * from live

union all

select * from archive
