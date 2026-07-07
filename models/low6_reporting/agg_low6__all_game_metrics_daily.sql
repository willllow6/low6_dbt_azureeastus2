{{
    config(
        materialized='view'
    )
}}

-- No ANALYTICS_ARCHIVE tables exist yet, so this is a pure passthrough of the
-- live table. TODO: once an archive source is defined in
-- _src_low6_reporting.yml, add an archive CTE here (null-padding columns the
-- archive doesn't have, aliasing any legacy column names) and union it in.

with

live as (
    select * from {{ ref('agg_low6__game_metrics_daily') }}
),

archive as (
    select * from {{ source('archive','game_metrics_daily') }}
)

select * from live

union all 

select * from archive
