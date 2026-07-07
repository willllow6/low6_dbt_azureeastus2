{{
    config(
        materialized='table'
    )
}}

-- Reuses agg_{domain}__cohort_retention_weekly directly where one already
-- exists and matches the standard contract (cfl_fantasy, bet365_uf,
-- opap_spintowin). Builds the rest inline from entries per the standard
-- entries -> cohort_weeks -> cohort_sizes -> user_cohort_activity -> retention
-- pattern. tenant_name is forced to null everywhere for consistency, even
-- where a reused model carries a hardcoded single-tenant value (e.g. opap's
-- 'OPAP') — see agg_low6__game_metrics_daily.sql for the same convention.
-- saracen and gana_gamezone split the same way as in agg_low6__game_metrics_daily.

with

--------------------------------------------------------------------------------
-- bet365_overunder
--------------------------------------------------------------------------------

bet365_overunder_entries as (
    select user_id, entry_date_et as entry_date
    from {{ ref('stg_bet365_overunder__entries') }}
),

bet365_overunder_cohort_weeks as (
    select user_id, date_trunc('week', min(entry_date))::date as cohort_week
    from bet365_overunder_entries group by 1
),

bet365_overunder_cohort_sizes as (
    select cohort_week, count(distinct user_id) as cohort_size
    from bet365_overunder_cohort_weeks group by 1
),

bet365_overunder_user_cohort_activity as (
    select
        cw.cohort_week,
        date_trunc('week', e.entry_date)::date as activity_week,
        e.user_id
    from bet365_overunder_entries e
    inner join bet365_overunder_cohort_weeks cw on e.user_id = cw.user_id
),

bet365_overunder_retention as (
    select
        uca.cohort_week,
        uca.activity_week,
        datediff('week', uca.cohort_week, uca.activity_week) as weeks_since_cohort,
        cs.cohort_size,
        count(distinct uca.user_id) as retained_users,
        round(count(distinct uca.user_id) / cs.cohort_size::float, 4) as retention_rate
    from bet365_overunder_user_cohort_activity uca
    inner join bet365_overunder_cohort_sizes cs on uca.cohort_week = cs.cohort_week
    group by 1, 2, 3, 4
),

game_cohort_bet365_overunder as (
    select
        cohort_week, activity_week,
        'bet365_overunder' as game_id,
        'Bet365 Over/Under' as game_name,
        'pickem' as game_type,
        'bet365' as client_id,
        'bet365_overunder' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        weeks_since_cohort, cohort_size, retained_users, retention_rate
    from bet365_overunder_retention
),

--------------------------------------------------------------------------------
-- bet99_picks
--------------------------------------------------------------------------------

bet99_picks_entries as (
    select user_id, entry_date_et as entry_date
    from {{ ref('bet99_picks__entries') }}
),

bet99_picks_cohort_weeks as (
    select user_id, date_trunc('week', min(entry_date))::date as cohort_week
    from bet99_picks_entries group by 1
),

bet99_picks_cohort_sizes as (
    select cohort_week, count(distinct user_id) as cohort_size
    from bet99_picks_cohort_weeks group by 1
),

bet99_picks_user_cohort_activity as (
    select
        cw.cohort_week,
        date_trunc('week', e.entry_date)::date as activity_week,
        e.user_id
    from bet99_picks_entries e
    inner join bet99_picks_cohort_weeks cw on e.user_id = cw.user_id
),

bet99_picks_retention as (
    select
        uca.cohort_week,
        uca.activity_week,
        datediff('week', uca.cohort_week, uca.activity_week) as weeks_since_cohort,
        cs.cohort_size,
        count(distinct uca.user_id) as retained_users,
        round(count(distinct uca.user_id) / cs.cohort_size::float, 4) as retention_rate
    from bet99_picks_user_cohort_activity uca
    inner join bet99_picks_cohort_sizes cs on uca.cohort_week = cs.cohort_week
    group by 1, 2, 3, 4
),

game_cohort_bet99_picks as (
    select
        cohort_week, activity_week,
        'bet99_picks' as game_id,
        'Bet99 Picks' as game_name,
        'pickem' as game_type,
        'bet99' as client_id,
        'bet99_picks' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        weeks_since_cohort, cohort_size, retained_users, retention_rate
    from bet99_picks_retention
),

--------------------------------------------------------------------------------
-- betway_picks
--------------------------------------------------------------------------------

betway_picks_entries as (
    select user_id, entry_date_et as entry_date
    from {{ ref('betway_picks__entries') }}
),

betway_picks_cohort_weeks as (
    select user_id, date_trunc('week', min(entry_date))::date as cohort_week
    from betway_picks_entries group by 1
),

betway_picks_cohort_sizes as (
    select cohort_week, count(distinct user_id) as cohort_size
    from betway_picks_cohort_weeks group by 1
),

betway_picks_user_cohort_activity as (
    select
        cw.cohort_week,
        date_trunc('week', e.entry_date)::date as activity_week,
        e.user_id
    from betway_picks_entries e
    inner join betway_picks_cohort_weeks cw on e.user_id = cw.user_id
),

betway_picks_retention as (
    select
        uca.cohort_week,
        uca.activity_week,
        datediff('week', uca.cohort_week, uca.activity_week) as weeks_since_cohort,
        cs.cohort_size,
        count(distinct uca.user_id) as retained_users,
        round(count(distinct uca.user_id) / cs.cohort_size::float, 4) as retention_rate
    from betway_picks_user_cohort_activity uca
    inner join betway_picks_cohort_sizes cs on uca.cohort_week = cs.cohort_week
    group by 1, 2, 3, 4
),

game_cohort_betway_picks as (
    select
        cohort_week, activity_week,
        'betway_picks' as game_id,
        'Betway Picks' as game_name,
        'pickem' as game_type,
        'betway' as client_id,
        'betway_picks' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        weeks_since_cohort, cohort_size, retained_users, retention_rate
    from betway_picks_retention
),

--------------------------------------------------------------------------------
-- saracen_pickem / saracen_bracket
--------------------------------------------------------------------------------

saracen_pickem_entries as (
    select sso_user_id as user_id, entry_date_et as entry_date
    from {{ ref('mart_saracen__entries') }}
    where game_type = 'pickem'
),

saracen_pickem_cohort_weeks as (
    select user_id, date_trunc('week', min(entry_date))::date as cohort_week
    from saracen_pickem_entries group by 1
),

saracen_pickem_cohort_sizes as (
    select cohort_week, count(distinct user_id) as cohort_size
    from saracen_pickem_cohort_weeks group by 1
),

saracen_pickem_user_cohort_activity as (
    select
        cw.cohort_week,
        date_trunc('week', e.entry_date)::date as activity_week,
        e.user_id
    from saracen_pickem_entries e
    inner join saracen_pickem_cohort_weeks cw on e.user_id = cw.user_id
),

saracen_pickem_retention as (
    select
        uca.cohort_week,
        uca.activity_week,
        datediff('week', uca.cohort_week, uca.activity_week) as weeks_since_cohort,
        cs.cohort_size,
        count(distinct uca.user_id) as retained_users,
        round(count(distinct uca.user_id) / cs.cohort_size::float, 4) as retention_rate
    from saracen_pickem_user_cohort_activity uca
    inner join saracen_pickem_cohort_sizes cs on uca.cohort_week = cs.cohort_week
    group by 1, 2, 3, 4
),

game_cohort_saracen_pickem as (
    select
        cohort_week, activity_week,
        'saracen_pickem' as game_id,
        'Saracen Pickem' as game_name,
        'pickem' as game_type,
        'saracen' as client_id,
        'saracen' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        weeks_since_cohort, cohort_size, retained_users, retention_rate
    from saracen_pickem_retention
),

saracen_bracket_entries as (
    select sso_user_id as user_id, entry_date_et as entry_date
    from {{ ref('mart_saracen__entries') }}
    where game_type = 'bracket'
),

saracen_bracket_cohort_weeks as (
    select user_id, date_trunc('week', min(entry_date))::date as cohort_week
    from saracen_bracket_entries group by 1
),

saracen_bracket_cohort_sizes as (
    select cohort_week, count(distinct user_id) as cohort_size
    from saracen_bracket_cohort_weeks group by 1
),

saracen_bracket_user_cohort_activity as (
    select
        cw.cohort_week,
        date_trunc('week', e.entry_date)::date as activity_week,
        e.user_id
    from saracen_bracket_entries e
    inner join saracen_bracket_cohort_weeks cw on e.user_id = cw.user_id
),

saracen_bracket_retention as (
    select
        uca.cohort_week,
        uca.activity_week,
        datediff('week', uca.cohort_week, uca.activity_week) as weeks_since_cohort,
        cs.cohort_size,
        count(distinct uca.user_id) as retained_users,
        round(count(distinct uca.user_id) / cs.cohort_size::float, 4) as retention_rate
    from saracen_bracket_user_cohort_activity uca
    inner join saracen_bracket_cohort_sizes cs on uca.cohort_week = cs.cohort_week
    group by 1, 2, 3, 4
),

game_cohort_saracen_bracket as (
    select
        cohort_week, activity_week,
        'saracen_bracket' as game_id,
        'Saracen Bracket' as game_name,
        'bracket' as game_type,
        'saracen' as client_id,
        'saracen' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        weeks_since_cohort, cohort_size, retained_users, retention_rate
    from saracen_bracket_retention
),

--------------------------------------------------------------------------------
-- bet99_bracket
--------------------------------------------------------------------------------

bet99_bracket_entries as (
    select user_id, created_date_et as entry_date
    from {{ ref('mart_bet99_bracket__entries') }}
),

bet99_bracket_cohort_weeks as (
    select user_id, date_trunc('week', min(entry_date))::date as cohort_week
    from bet99_bracket_entries group by 1
),

bet99_bracket_cohort_sizes as (
    select cohort_week, count(distinct user_id) as cohort_size
    from bet99_bracket_cohort_weeks group by 1
),

bet99_bracket_user_cohort_activity as (
    select
        cw.cohort_week,
        date_trunc('week', e.entry_date)::date as activity_week,
        e.user_id
    from bet99_bracket_entries e
    inner join bet99_bracket_cohort_weeks cw on e.user_id = cw.user_id
),

bet99_bracket_retention as (
    select
        uca.cohort_week,
        uca.activity_week,
        datediff('week', uca.cohort_week, uca.activity_week) as weeks_since_cohort,
        cs.cohort_size,
        count(distinct uca.user_id) as retained_users,
        round(count(distinct uca.user_id) / cs.cohort_size::float, 4) as retention_rate
    from bet99_bracket_user_cohort_activity uca
    inner join bet99_bracket_cohort_sizes cs on uca.cohort_week = cs.cohort_week
    group by 1, 2, 3, 4
),

game_cohort_bet99_bracket as (
    select
        cohort_week, activity_week,
        'bet99_bracket' as game_id,
        'Bet99 Bracket' as game_name,
        'bracket' as game_type,
        'bet99' as client_id,
        'bet99_bracket' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        weeks_since_cohort, cohort_size, retained_users, retention_rate
    from bet99_bracket_retention
),

--------------------------------------------------------------------------------
-- gana_predictor / gana_survivor / gana_bracket
--------------------------------------------------------------------------------

gana_predictor_entries as (
    select
        user_id,
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', entered_at) as date) as entry_date
    from {{ ref('fct_gana_gamezone__predictor_entries') }}
),

gana_predictor_cohort_weeks as (
    select user_id, date_trunc('week', min(entry_date))::date as cohort_week
    from gana_predictor_entries group by 1
),

gana_predictor_cohort_sizes as (
    select cohort_week, count(distinct user_id) as cohort_size
    from gana_predictor_cohort_weeks group by 1
),

gana_predictor_user_cohort_activity as (
    select
        cw.cohort_week,
        date_trunc('week', e.entry_date)::date as activity_week,
        e.user_id
    from gana_predictor_entries e
    inner join gana_predictor_cohort_weeks cw on e.user_id = cw.user_id
),

gana_predictor_retention as (
    select
        uca.cohort_week,
        uca.activity_week,
        datediff('week', uca.cohort_week, uca.activity_week) as weeks_since_cohort,
        cs.cohort_size,
        count(distinct uca.user_id) as retained_users,
        round(count(distinct uca.user_id) / cs.cohort_size::float, 4) as retention_rate
    from gana_predictor_user_cohort_activity uca
    inner join gana_predictor_cohort_sizes cs on uca.cohort_week = cs.cohort_week
    group by 1, 2, 3, 4
),

game_cohort_gana_predictor as (
    select
        cohort_week, activity_week,
        'gana_predictor' as game_id,
        'Gana Predictor' as game_name,
        'pickem' as game_type,
        'gana' as client_id,
        'gana_gamezone' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        weeks_since_cohort, cohort_size, retained_users, retention_rate
    from gana_predictor_retention
),

gana_survivor_entries as (
    select
        user_id,
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', selected_at) as date) as entry_date
    from {{ ref('fct_gana_gamezone__survivor_entries') }}
    where selected_country_id is not null
),

gana_survivor_cohort_weeks as (
    select user_id, date_trunc('week', min(entry_date))::date as cohort_week
    from gana_survivor_entries group by 1
),

gana_survivor_cohort_sizes as (
    select cohort_week, count(distinct user_id) as cohort_size
    from gana_survivor_cohort_weeks group by 1
),

gana_survivor_user_cohort_activity as (
    select
        cw.cohort_week,
        date_trunc('week', e.entry_date)::date as activity_week,
        e.user_id
    from gana_survivor_entries e
    inner join gana_survivor_cohort_weeks cw on e.user_id = cw.user_id
),

gana_survivor_retention as (
    select
        uca.cohort_week,
        uca.activity_week,
        datediff('week', uca.cohort_week, uca.activity_week) as weeks_since_cohort,
        cs.cohort_size,
        count(distinct uca.user_id) as retained_users,
        round(count(distinct uca.user_id) / cs.cohort_size::float, 4) as retention_rate
    from gana_survivor_user_cohort_activity uca
    inner join gana_survivor_cohort_sizes cs on uca.cohort_week = cs.cohort_week
    group by 1, 2, 3, 4
),

game_cohort_gana_survivor as (
    select
        cohort_week, activity_week,
        'gana_survivor' as game_id,
        'Gana Survivor' as game_name,
        'streak' as game_type,
        'gana' as client_id,
        'gana_gamezone' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        weeks_since_cohort, cohort_size, retained_users, retention_rate
    from gana_survivor_retention
),

gana_bracket_entries as (
    select
        user_id,
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', entered_at) as date) as entry_date
    from {{ ref('fct_gana_gamezone__bracket_entries') }}
),

gana_bracket_cohort_weeks as (
    select user_id, date_trunc('week', min(entry_date))::date as cohort_week
    from gana_bracket_entries group by 1
),

gana_bracket_cohort_sizes as (
    select cohort_week, count(distinct user_id) as cohort_size
    from gana_bracket_cohort_weeks group by 1
),

gana_bracket_user_cohort_activity as (
    select
        cw.cohort_week,
        date_trunc('week', e.entry_date)::date as activity_week,
        e.user_id
    from gana_bracket_entries e
    inner join gana_bracket_cohort_weeks cw on e.user_id = cw.user_id
),

gana_bracket_retention as (
    select
        uca.cohort_week,
        uca.activity_week,
        datediff('week', uca.cohort_week, uca.activity_week) as weeks_since_cohort,
        cs.cohort_size,
        count(distinct uca.user_id) as retained_users,
        round(count(distinct uca.user_id) / cs.cohort_size::float, 4) as retention_rate
    from gana_bracket_user_cohort_activity uca
    inner join gana_bracket_cohort_sizes cs on uca.cohort_week = cs.cohort_week
    group by 1, 2, 3, 4
),

game_cohort_gana_bracket as (
    select
        cohort_week, activity_week,
        'gana_bracket' as game_id,
        'Gana Bracket' as game_name,
        'bracket' as game_type,
        'gana' as client_id,
        'gana_gamezone' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        weeks_since_cohort, cohort_size, retained_users, retention_rate
    from gana_bracket_retention
),

--------------------------------------------------------------------------------
-- Reused domain-level cohort models (already spec-conformant)
--------------------------------------------------------------------------------

game_cohort_cfl_fantasy as (
    select
        cohort_week, activity_week,
        'cfl_fantasy' as game_id,
        'CFL Fantasy' as game_name,
        'fantasy' as game_type,
        client_id,
        'cfl_fantasy' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        weeks_since_cohort, cohort_size, retained_users, retention_rate
    from {{ ref('agg_cfl_fantasy__cohort_retention_weekly') }}
),

game_cohort_bet365_uf as (
    select
        cohort_week, activity_week,
        'bet365_uf' as game_id,
        'Bet365 Ultimate Fan' as game_name,
        'fantasy' as game_type,
        client_id,
        'bet365_uf' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        weeks_since_cohort, cohort_size, retained_users, retention_rate
    from {{ ref('agg_bet365_uf__cohort_retention_weekly') }}
),

game_cohort_opap_spintowin as (
    select
        cohort_week, activity_week,
        'opap_spintowin' as game_id,
        'OPAP Spin to Win' as game_name,
        'spin_to_win' as game_type,
        client_id,
        'opap_spintowin' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        weeks_since_cohort, cohort_size, retained_users, retention_rate
    from {{ ref('agg_opap_spintowin__cohort_retention_weekly') }}
)

select * from game_cohort_bet365_overunder
union all
select * from game_cohort_bet99_picks
union all
select * from game_cohort_betway_picks
union all
select * from game_cohort_saracen_pickem
union all
select * from game_cohort_saracen_bracket
union all
select * from game_cohort_bet99_bracket
union all
select * from game_cohort_gana_predictor
union all
select * from game_cohort_gana_survivor
union all
select * from game_cohort_gana_bracket
union all
select * from game_cohort_cfl_fantasy
union all
select * from game_cohort_bet365_uf
union all
select * from game_cohort_opap_spintowin
