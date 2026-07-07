{{
    config(
        materialized='table'
    )
}}

-- One CTE group per game_id, per the low6 cross-game reporting Build pattern.
-- saracen and gana_gamezone each split into multiple game_ids because a single
-- domain produces more than one game_type (saracen: pickem + bracket;
-- gana_gamezone: predictor/survivor/bracket share one source but no unified
-- entries fact). elf_collectyourelf is out of scope (no entries/contest concept).
-- bet365_uf is treated as single-tenant here even though it has real tenant_id
-- data in source — dim_bet365_uf__tenants doesn't exist yet, that's a follow-up.

with

--------------------------------------------------------------------------------
-- bet365_overunder (pickem, single-tenant)
--------------------------------------------------------------------------------

bet365_overunder_ents_raw as (
    select user_id, entry_date_et as date_day
    from {{ ref('stg_bet365_overunder__entries') }}
),

bet365_overunder_regs_raw as (
    select registration_date_et as date_day
    from {{ ref('stg_bet365_overunder__users') }}
),

bet365_overunder_bounds as (
    select min(date_day) as min_date from bet365_overunder_ents_raw
),

bet365_overunder_spine as (
    select dateadd(day, seq4(), (select min_date from bet365_overunder_bounds)) as date_day
    from table(generator(rowcount => 3000))
    where date_day <= current_date()
),

bet365_overunder_daily as (
    select date_day, count(*) as total_entries, count(distinct user_id) as unique_entrants
    from bet365_overunder_ents_raw group by 1
),

bet365_overunder_wau as (
    select s.date_day, count(distinct e.user_id) as wau
    from bet365_overunder_spine s
    left join bet365_overunder_ents_raw e
        on e.date_day between dateadd(day, -6, s.date_day) and s.date_day
    group by 1
),

bet365_overunder_mau as (
    select s.date_day, count(distinct e.user_id) as mau
    from bet365_overunder_spine s
    left join bet365_overunder_ents_raw e
        on e.date_day between dateadd(day, -27, s.date_day) and s.date_day
    group by 1
),

bet365_overunder_regs as (
    select date_day, count(*) as registrations
    from bet365_overunder_regs_raw group by 1
),

bet365_overunder_first_entries_daily as (
    select first_date as date_day, count(*) as first_entries
    from (
        select user_id, min(date_day) as first_date
        from bet365_overunder_ents_raw group by 1
    )
    group by 1
),

game_bet365_overunder as (
    select
        s.date_day,
        'bet365_overunder' as game_id,
        'Bet365 Over/Under' as game_name,
        'pickem' as game_type,
        'bet365' as client_id,
        'bet365_overunder' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        coalesce(r.registrations, 0) as registrations,
        coalesce(d.total_entries, 0) as entries,
        coalesce(d.unique_entrants, 0) as dau,
        coalesce(w.wau, 0) as wau,
        coalesce(m.mau, 0) as mau,
        coalesce(fe.first_entries, 0) as first_entries,
        cast(null as integer) as purchases,
        cast(null as number) as gross_revenue,
        cast(null as integer) as dpu,
        cast(null as integer) as first_purchases,
        cast(null as integer) as wpu,
        cast(null as integer) as mpu
    from bet365_overunder_spine s
    left join bet365_overunder_daily d on s.date_day = d.date_day
    left join bet365_overunder_wau w on s.date_day = w.date_day
    left join bet365_overunder_mau m on s.date_day = m.date_day
    left join bet365_overunder_regs r on s.date_day = r.date_day
    left join bet365_overunder_first_entries_daily fe on s.date_day = fe.date_day
),

--------------------------------------------------------------------------------
-- bet99_picks (pickem, single-tenant)
--------------------------------------------------------------------------------

bet99_picks_ents_raw as (
    select user_id, entry_date_et as date_day
    from {{ ref('bet99_picks__entries') }}
),

bet99_picks_regs_raw as (
    select registration_date_et as date_day
    from {{ ref('stg_bet99_picks__users') }}
),

bet99_picks_bounds as (
    select min(date_day) as min_date from bet99_picks_ents_raw
),

bet99_picks_spine as (
    select dateadd(day, seq4(), (select min_date from bet99_picks_bounds)) as date_day
    from table(generator(rowcount => 3000))
    where date_day <= current_date()
),

bet99_picks_daily as (
    select date_day, count(*) as total_entries, count(distinct user_id) as unique_entrants
    from bet99_picks_ents_raw group by 1
),

bet99_picks_wau as (
    select s.date_day, count(distinct e.user_id) as wau
    from bet99_picks_spine s
    left join bet99_picks_ents_raw e
        on e.date_day between dateadd(day, -6, s.date_day) and s.date_day
    group by 1
),

bet99_picks_mau as (
    select s.date_day, count(distinct e.user_id) as mau
    from bet99_picks_spine s
    left join bet99_picks_ents_raw e
        on e.date_day between dateadd(day, -27, s.date_day) and s.date_day
    group by 1
),

bet99_picks_regs as (
    select date_day, count(*) as registrations
    from bet99_picks_regs_raw group by 1
),

bet99_picks_first_entries_daily as (
    select first_date as date_day, count(*) as first_entries
    from (
        select user_id, min(date_day) as first_date
        from bet99_picks_ents_raw group by 1
    )
    group by 1
),

game_bet99_picks as (
    select
        s.date_day,
        'bet99_picks' as game_id,
        'Bet99 Picks' as game_name,
        'pickem' as game_type,
        'bet99' as client_id,
        'bet99_picks' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        coalesce(r.registrations, 0) as registrations,
        coalesce(d.total_entries, 0) as entries,
        coalesce(d.unique_entrants, 0) as dau,
        coalesce(w.wau, 0) as wau,
        coalesce(m.mau, 0) as mau,
        coalesce(fe.first_entries, 0) as first_entries,
        cast(null as integer) as purchases,
        cast(null as number) as gross_revenue,
        cast(null as integer) as dpu,
        cast(null as integer) as first_purchases,
        cast(null as integer) as wpu,
        cast(null as integer) as mpu
    from bet99_picks_spine s
    left join bet99_picks_daily d on s.date_day = d.date_day
    left join bet99_picks_wau w on s.date_day = w.date_day
    left join bet99_picks_mau m on s.date_day = m.date_day
    left join bet99_picks_regs r on s.date_day = r.date_day
    left join bet99_picks_first_entries_daily fe on s.date_day = fe.date_day
),

--------------------------------------------------------------------------------
-- betway_picks (pickem, single-tenant)
--------------------------------------------------------------------------------

betway_picks_ents_raw as (
    select user_id, entry_date_et as date_day
    from {{ ref('betway_picks__entries') }}
),

betway_picks_regs_raw as (
    select registration_date_et as date_day
    from {{ ref('stg_betway_picks__users') }}
),

betway_picks_bounds as (
    select min(date_day) as min_date from betway_picks_ents_raw
),

betway_picks_spine as (
    select dateadd(day, seq4(), (select min_date from betway_picks_bounds)) as date_day
    from table(generator(rowcount => 3000))
    where date_day <= current_date()
),

betway_picks_daily as (
    select date_day, count(*) as total_entries, count(distinct user_id) as unique_entrants
    from betway_picks_ents_raw group by 1
),

betway_picks_wau as (
    select s.date_day, count(distinct e.user_id) as wau
    from betway_picks_spine s
    left join betway_picks_ents_raw e
        on e.date_day between dateadd(day, -6, s.date_day) and s.date_day
    group by 1
),

betway_picks_mau as (
    select s.date_day, count(distinct e.user_id) as mau
    from betway_picks_spine s
    left join betway_picks_ents_raw e
        on e.date_day between dateadd(day, -27, s.date_day) and s.date_day
    group by 1
),

betway_picks_regs as (
    select date_day, count(*) as registrations
    from betway_picks_regs_raw group by 1
),

betway_picks_first_entries_daily as (
    select first_date as date_day, count(*) as first_entries
    from (
        select user_id, min(date_day) as first_date
        from betway_picks_ents_raw group by 1
    )
    group by 1
),

game_betway_picks as (
    select
        s.date_day,
        'betway_picks' as game_id,
        'Betway Picks' as game_name,
        'pickem' as game_type,
        'betway' as client_id,
        'betway_picks' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        coalesce(r.registrations, 0) as registrations,
        coalesce(d.total_entries, 0) as entries,
        coalesce(d.unique_entrants, 0) as dau,
        coalesce(w.wau, 0) as wau,
        coalesce(m.mau, 0) as mau,
        coalesce(fe.first_entries, 0) as first_entries,
        cast(null as integer) as purchases,
        cast(null as number) as gross_revenue,
        cast(null as integer) as dpu,
        cast(null as integer) as first_purchases,
        cast(null as integer) as wpu,
        cast(null as integer) as mpu
    from betway_picks_spine s
    left join betway_picks_daily d on s.date_day = d.date_day
    left join betway_picks_wau w on s.date_day = w.date_day
    left join betway_picks_mau m on s.date_day = m.date_day
    left join betway_picks_regs r on s.date_day = r.date_day
    left join betway_picks_first_entries_daily fe on s.date_day = fe.date_day
),

--------------------------------------------------------------------------------
-- saracen_pickem (pickem, single-tenant)
-- saracen's picks sub-game is small (4 entries at time of writing) but live in
-- prod (int_saracen__entries_unioned.sql no longer filters it out) — split from
-- saracen_bracket by mart_saracen__entries.game_type rather than assuming
-- bracket-only. Pickem registrations are always 0 today: the user identity
-- spine (int_saracen__user_identity_spine.sql) still hardcodes `where 1 = 0` on
-- picks users, so pickem_created_date_et is always null.
--------------------------------------------------------------------------------

saracen_pickem_ents_raw as (
    select sso_user_id as user_id, entry_date_et as date_day
    from {{ ref('mart_saracen__entries') }}
    where game_type = 'pickem'
),

saracen_pickem_regs_raw as (
    select pickem_created_date_et as date_day
    from {{ ref('dim_saracen__users') }}
    where pickem_created_date_et is not null
),

saracen_pickem_bounds as (
    select min(date_day) as min_date from saracen_pickem_ents_raw
),

saracen_pickem_spine as (
    select dateadd(day, seq4(), (select min_date from saracen_pickem_bounds)) as date_day
    from table(generator(rowcount => 3000))
    where date_day <= current_date()
),

saracen_pickem_daily as (
    select date_day, count(*) as total_entries, count(distinct user_id) as unique_entrants
    from saracen_pickem_ents_raw group by 1
),

saracen_pickem_wau as (
    select s.date_day, count(distinct e.user_id) as wau
    from saracen_pickem_spine s
    left join saracen_pickem_ents_raw e
        on e.date_day between dateadd(day, -6, s.date_day) and s.date_day
    group by 1
),

saracen_pickem_mau as (
    select s.date_day, count(distinct e.user_id) as mau
    from saracen_pickem_spine s
    left join saracen_pickem_ents_raw e
        on e.date_day between dateadd(day, -27, s.date_day) and s.date_day
    group by 1
),

saracen_pickem_regs as (
    select date_day, count(*) as registrations
    from saracen_pickem_regs_raw group by 1
),

saracen_pickem_first_entries_daily as (
    select first_date as date_day, count(*) as first_entries
    from (
        select user_id, min(date_day) as first_date
        from saracen_pickem_ents_raw group by 1
    )
    group by 1
),

game_saracen_pickem as (
    select
        s.date_day,
        'saracen_pickem' as game_id,
        'Saracen Pickem' as game_name,
        'pickem' as game_type,
        'saracen' as client_id,
        'saracen' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        coalesce(r.registrations, 0) as registrations,
        coalesce(d.total_entries, 0) as entries,
        coalesce(d.unique_entrants, 0) as dau,
        coalesce(w.wau, 0) as wau,
        coalesce(m.mau, 0) as mau,
        coalesce(fe.first_entries, 0) as first_entries,
        cast(null as integer) as purchases,
        cast(null as number) as gross_revenue,
        cast(null as integer) as dpu,
        cast(null as integer) as first_purchases,
        cast(null as integer) as wpu,
        cast(null as integer) as mpu
    from saracen_pickem_spine s
    left join saracen_pickem_daily d on s.date_day = d.date_day
    left join saracen_pickem_wau w on s.date_day = w.date_day
    left join saracen_pickem_mau m on s.date_day = m.date_day
    left join saracen_pickem_regs r on s.date_day = r.date_day
    left join saracen_pickem_first_entries_daily fe on s.date_day = fe.date_day
),

--------------------------------------------------------------------------------
-- saracen_bracket (bracket, single-tenant)
--------------------------------------------------------------------------------

saracen_bracket_ents_raw as (
    select sso_user_id as user_id, entry_date_et as date_day
    from {{ ref('mart_saracen__entries') }}
    where game_type = 'bracket'
),

saracen_bracket_regs_raw as (
    select bracket_created_date_et as date_day
    from {{ ref('dim_saracen__users') }}
    where bracket_created_date_et is not null
),

saracen_bracket_bounds as (
    select min(date_day) as min_date from saracen_bracket_ents_raw
),

saracen_bracket_spine as (
    select dateadd(day, seq4(), (select min_date from saracen_bracket_bounds)) as date_day
    from table(generator(rowcount => 3000))
    where date_day <= current_date()
),

saracen_bracket_daily as (
    select date_day, count(*) as total_entries, count(distinct user_id) as unique_entrants
    from saracen_bracket_ents_raw group by 1
),

saracen_bracket_wau as (
    select s.date_day, count(distinct e.user_id) as wau
    from saracen_bracket_spine s
    left join saracen_bracket_ents_raw e
        on e.date_day between dateadd(day, -6, s.date_day) and s.date_day
    group by 1
),

saracen_bracket_mau as (
    select s.date_day, count(distinct e.user_id) as mau
    from saracen_bracket_spine s
    left join saracen_bracket_ents_raw e
        on e.date_day between dateadd(day, -27, s.date_day) and s.date_day
    group by 1
),

saracen_bracket_regs as (
    select date_day, count(*) as registrations
    from saracen_bracket_regs_raw group by 1
),

saracen_bracket_first_entries_daily as (
    select first_date as date_day, count(*) as first_entries
    from (
        select user_id, min(date_day) as first_date
        from saracen_bracket_ents_raw group by 1
    )
    group by 1
),

game_saracen_bracket as (
    select
        s.date_day,
        'saracen_bracket' as game_id,
        'Saracen Bracket' as game_name,
        'bracket' as game_type,
        'saracen' as client_id,
        'saracen' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        coalesce(r.registrations, 0) as registrations,
        coalesce(d.total_entries, 0) as entries,
        coalesce(d.unique_entrants, 0) as dau,
        coalesce(w.wau, 0) as wau,
        coalesce(m.mau, 0) as mau,
        coalesce(fe.first_entries, 0) as first_entries,
        cast(null as integer) as purchases,
        cast(null as number) as gross_revenue,
        cast(null as integer) as dpu,
        cast(null as integer) as first_purchases,
        cast(null as integer) as wpu,
        cast(null as integer) as mpu
    from saracen_bracket_spine s
    left join saracen_bracket_daily d on s.date_day = d.date_day
    left join saracen_bracket_wau w on s.date_day = w.date_day
    left join saracen_bracket_mau m on s.date_day = m.date_day
    left join saracen_bracket_regs r on s.date_day = r.date_day
    left join saracen_bracket_first_entries_daily fe on s.date_day = fe.date_day
),

--------------------------------------------------------------------------------
-- bet99_bracket (bracket, single-tenant)
--------------------------------------------------------------------------------

bet99_bracket_ents_raw as (
    select user_id, created_date_et as date_day
    from {{ ref('mart_bet99_bracket__entries') }}
),

bet99_bracket_regs_raw as (
    select created_date_et as date_day
    from {{ ref('dim_bet99_bracket__users') }}
),

bet99_bracket_bounds as (
    select min(date_day) as min_date from bet99_bracket_ents_raw
),

bet99_bracket_spine as (
    select dateadd(day, seq4(), (select min_date from bet99_bracket_bounds)) as date_day
    from table(generator(rowcount => 3000))
    where date_day <= current_date()
),

bet99_bracket_daily as (
    select date_day, count(*) as total_entries, count(distinct user_id) as unique_entrants
    from bet99_bracket_ents_raw group by 1
),

bet99_bracket_wau as (
    select s.date_day, count(distinct e.user_id) as wau
    from bet99_bracket_spine s
    left join bet99_bracket_ents_raw e
        on e.date_day between dateadd(day, -6, s.date_day) and s.date_day
    group by 1
),

bet99_bracket_mau as (
    select s.date_day, count(distinct e.user_id) as mau
    from bet99_bracket_spine s
    left join bet99_bracket_ents_raw e
        on e.date_day between dateadd(day, -27, s.date_day) and s.date_day
    group by 1
),

bet99_bracket_regs as (
    select date_day, count(*) as registrations
    from bet99_bracket_regs_raw group by 1
),

bet99_bracket_first_entries_daily as (
    select first_date as date_day, count(*) as first_entries
    from (
        select user_id, min(date_day) as first_date
        from bet99_bracket_ents_raw group by 1
    )
    group by 1
),

game_bet99_bracket as (
    select
        s.date_day,
        'bet99_bracket' as game_id,
        'Bet99 Bracket' as game_name,
        'bracket' as game_type,
        'bet99' as client_id,
        'bet99_bracket' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        coalesce(r.registrations, 0) as registrations,
        coalesce(d.total_entries, 0) as entries,
        coalesce(d.unique_entrants, 0) as dau,
        coalesce(w.wau, 0) as wau,
        coalesce(m.mau, 0) as mau,
        coalesce(fe.first_entries, 0) as first_entries,
        cast(null as integer) as purchases,
        cast(null as number) as gross_revenue,
        cast(null as integer) as dpu,
        cast(null as integer) as first_purchases,
        cast(null as integer) as wpu,
        cast(null as integer) as mpu
    from bet99_bracket_spine s
    left join bet99_bracket_daily d on s.date_day = d.date_day
    left join bet99_bracket_wau w on s.date_day = w.date_day
    left join bet99_bracket_mau m on s.date_day = m.date_day
    left join bet99_bracket_regs r on s.date_day = r.date_day
    left join bet99_bracket_first_entries_daily fe on s.date_day = fe.date_day
),

--------------------------------------------------------------------------------
-- bet365_uf (fantasy, treated as single-tenant for this build — see plan notes;
-- the only domain with IAP revenue). purchase_currency is genuinely mixed
-- (USD/CAD/PKR seen in source) and there is no FX conversion in this project,
-- so gross_revenue is a raw cross-currency sum — a known limitation of the
-- cross-game standard interface, which has no currency dimension. Treat this
-- number as indicative only until FX-normalized.
--------------------------------------------------------------------------------

bet365_uf_ents_raw as (
    select
        user_id,
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', entered_at) as date) as date_day
    from {{ ref('fct_bet365_uf__entries') }}
),

bet365_uf_regs_raw as (
    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', registered_at) as date) as date_day
    from {{ ref('stg_bet365_uf__users') }}
    where not is_tester
),

bet365_uf_purchases_raw as (
    select
        user_id,
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', purchased_at) as date) as date_day,
        purchase_price
    from {{ ref('mart_bet365_uf__app_store_purchases') }}
    where is_tester = false
),

bet365_uf_bounds as (
    select min(date_day) as min_date from bet365_uf_ents_raw
),

bet365_uf_spine as (
    select dateadd(day, seq4(), (select min_date from bet365_uf_bounds)) as date_day
    from table(generator(rowcount => 3000))
    where date_day <= current_date()
),

bet365_uf_daily as (
    select date_day, count(*) as total_entries, count(distinct user_id) as unique_entrants
    from bet365_uf_ents_raw group by 1
),

bet365_uf_wau as (
    select s.date_day, count(distinct e.user_id) as wau
    from bet365_uf_spine s
    left join bet365_uf_ents_raw e
        on e.date_day between dateadd(day, -6, s.date_day) and s.date_day
    group by 1
),

bet365_uf_mau as (
    select s.date_day, count(distinct e.user_id) as mau
    from bet365_uf_spine s
    left join bet365_uf_ents_raw e
        on e.date_day between dateadd(day, -27, s.date_day) and s.date_day
    group by 1
),

bet365_uf_regs as (
    select date_day, count(*) as registrations
    from bet365_uf_regs_raw group by 1
),

bet365_uf_first_entries_daily as (
    select first_date as date_day, count(*) as first_entries
    from (
        select user_id, min(date_day) as first_date
        from bet365_uf_ents_raw group by 1
    )
    group by 1
),

bet365_uf_daily_rev as (
    select
        date_day,
        count(*) as purchases,
        count(distinct user_id) as dpu,
        sum(purchase_price) as gross_revenue
    from bet365_uf_purchases_raw group by 1
),

bet365_uf_first_purchases_daily as (
    select first_date as date_day, count(*) as first_purchases
    from (
        select user_id, min(date_day) as first_date
        from bet365_uf_purchases_raw group by 1
    )
    group by 1
),

bet365_uf_wpu as (
    select s.date_day, count(distinct p.user_id) as wpu
    from bet365_uf_spine s
    left join bet365_uf_purchases_raw p
        on p.date_day between dateadd(day, -6, s.date_day) and s.date_day
    group by 1
),

bet365_uf_mpu as (
    select s.date_day, count(distinct p.user_id) as mpu
    from bet365_uf_spine s
    left join bet365_uf_purchases_raw p
        on p.date_day between dateadd(day, -27, s.date_day) and s.date_day
    group by 1
),

game_bet365_uf as (
    select
        s.date_day,
        'bet365_uf' as game_id,
        'Bet365 Ultimate Fan' as game_name,
        'fantasy' as game_type,
        'bet365' as client_id,
        'bet365_uf' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        coalesce(r.registrations, 0) as registrations,
        coalesce(d.total_entries, 0) as entries,
        coalesce(d.unique_entrants, 0) as dau,
        coalesce(w.wau, 0) as wau,
        coalesce(m.mau, 0) as mau,
        coalesce(fe.first_entries, 0) as first_entries,
        coalesce(rev.purchases, 0) as purchases,
        coalesce(rev.gross_revenue, 0) as gross_revenue,
        coalesce(rev.dpu, 0) as dpu,
        coalesce(fp.first_purchases, 0) as first_purchases,
        coalesce(wpu.wpu, 0) as wpu,
        coalesce(mpu.mpu, 0) as mpu
    from bet365_uf_spine s
    left join bet365_uf_daily d on s.date_day = d.date_day
    left join bet365_uf_wau w on s.date_day = w.date_day
    left join bet365_uf_mau m on s.date_day = m.date_day
    left join bet365_uf_regs r on s.date_day = r.date_day
    left join bet365_uf_first_entries_daily fe on s.date_day = fe.date_day
    left join bet365_uf_daily_rev rev on s.date_day = rev.date_day
    left join bet365_uf_first_purchases_daily fp on s.date_day = fp.date_day
    left join bet365_uf_wpu wpu on s.date_day = wpu.date_day
    left join bet365_uf_mpu mpu on s.date_day = mpu.date_day
),

--------------------------------------------------------------------------------
-- cfl_fantasy (fantasy, single-tenant, already fully canonical)
--------------------------------------------------------------------------------

cfl_fantasy_ents_raw as (
    select
        user_id,
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', entered_at) as date) as date_day
    from {{ ref('fct_cfl_fantasy__entries') }}
    where is_auto_team = false
),

cfl_fantasy_bounds as (
    select min(date_day) as min_date from cfl_fantasy_ents_raw
),

cfl_fantasy_spine as (
    select dateadd(day, seq4(), (select min_date from cfl_fantasy_bounds)) as date_day
    from table(generator(rowcount => 3000))
    where date_day <= current_date()
),

cfl_fantasy_daily as (
    select date_day, count(*) as total_entries, count(distinct user_id) as unique_entrants
    from cfl_fantasy_ents_raw group by 1
),

cfl_fantasy_wau as (
    select s.date_day, count(distinct e.user_id) as wau
    from cfl_fantasy_spine s
    left join cfl_fantasy_ents_raw e
        on e.date_day between dateadd(day, -6, s.date_day) and s.date_day
    group by 1
),

cfl_fantasy_mau as (
    select s.date_day, count(distinct e.user_id) as mau
    from cfl_fantasy_spine s
    left join cfl_fantasy_ents_raw e
        on e.date_day between dateadd(day, -27, s.date_day) and s.date_day
    group by 1
),

cfl_fantasy_regs as (
    select date_day, sum(new_registrations) as registrations
    from {{ ref('agg_cfl_fantasy__registration_metrics_daily') }}
    group by 1
),

cfl_fantasy_first_entries_daily as (
    select first_date as date_day, count(*) as first_entries
    from (
        select user_id, min(date_day) as first_date
        from cfl_fantasy_ents_raw group by 1
    )
    group by 1
),

game_cfl_fantasy as (
    select
        s.date_day,
        'cfl_fantasy' as game_id,
        'CFL Fantasy' as game_name,
        'fantasy' as game_type,
        'cfl' as client_id,
        'cfl_fantasy' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        coalesce(r.registrations, 0) as registrations,
        coalesce(d.total_entries, 0) as entries,
        coalesce(d.unique_entrants, 0) as dau,
        coalesce(w.wau, 0) as wau,
        coalesce(m.mau, 0) as mau,
        coalesce(fe.first_entries, 0) as first_entries,
        cast(null as integer) as purchases,
        cast(null as number) as gross_revenue,
        cast(null as integer) as dpu,
        cast(null as integer) as first_purchases,
        cast(null as integer) as wpu,
        cast(null as integer) as mpu
    from cfl_fantasy_spine s
    left join cfl_fantasy_daily d on s.date_day = d.date_day
    left join cfl_fantasy_wau w on s.date_day = w.date_day
    left join cfl_fantasy_mau m on s.date_day = m.date_day
    left join cfl_fantasy_regs r on s.date_day = r.date_day
    left join cfl_fantasy_first_entries_daily fe on s.date_day = fe.date_day
),

--------------------------------------------------------------------------------
-- opap_spintowin (spin_to_win, single-tenant)
-- game_type='spin_to_win' is the domain's real hardcoded value, not previously
-- in the canonical enum — added to the enum as part of this build rather than
-- remapped to the nearest existing value.
--------------------------------------------------------------------------------

opap_spintowin_ents_raw as (
    select user_id, entry_date_et as date_day
    from {{ ref('fct_opap_spintowin__entries') }}
),

opap_spintowin_regs_raw as (
    select registration_date_et as date_day
    from {{ ref('dim_opap_spintowin__users') }}
),

opap_spintowin_bounds as (
    select min(date_day) as min_date from opap_spintowin_ents_raw
),

opap_spintowin_spine as (
    select dateadd(day, seq4(), (select min_date from opap_spintowin_bounds)) as date_day
    from table(generator(rowcount => 3000))
    where date_day <= current_date()
),

opap_spintowin_daily as (
    select date_day, count(*) as total_entries, count(distinct user_id) as unique_entrants
    from opap_spintowin_ents_raw group by 1
),

opap_spintowin_wau as (
    select s.date_day, count(distinct e.user_id) as wau
    from opap_spintowin_spine s
    left join opap_spintowin_ents_raw e
        on e.date_day between dateadd(day, -6, s.date_day) and s.date_day
    group by 1
),

opap_spintowin_mau as (
    select s.date_day, count(distinct e.user_id) as mau
    from opap_spintowin_spine s
    left join opap_spintowin_ents_raw e
        on e.date_day between dateadd(day, -27, s.date_day) and s.date_day
    group by 1
),

opap_spintowin_regs as (
    select date_day, count(*) as registrations
    from opap_spintowin_regs_raw group by 1
),

opap_spintowin_first_entries_daily as (
    select first_date as date_day, count(*) as first_entries
    from (
        select user_id, min(date_day) as first_date
        from opap_spintowin_ents_raw group by 1
    )
    group by 1
),

game_opap_spintowin as (
    select
        s.date_day,
        'opap_spintowin' as game_id,
        'OPAP Spin to Win' as game_name,
        'spin_to_win' as game_type,
        'opap' as client_id,
        'opap_spintowin' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        coalesce(r.registrations, 0) as registrations,
        coalesce(d.total_entries, 0) as entries,
        coalesce(d.unique_entrants, 0) as dau,
        coalesce(w.wau, 0) as wau,
        coalesce(m.mau, 0) as mau,
        coalesce(fe.first_entries, 0) as first_entries,
        cast(null as integer) as purchases,
        cast(null as number) as gross_revenue,
        cast(null as integer) as dpu,
        cast(null as integer) as first_purchases,
        cast(null as integer) as wpu,
        cast(null as integer) as mpu
    from opap_spintowin_spine s
    left join opap_spintowin_daily d on s.date_day = d.date_day
    left join opap_spintowin_wau w on s.date_day = w.date_day
    left join opap_spintowin_mau m on s.date_day = m.date_day
    left join opap_spintowin_regs r on s.date_day = r.date_day
    left join opap_spintowin_first_entries_daily fe on s.date_day = fe.date_day
),

--------------------------------------------------------------------------------
-- gana_predictor / gana_survivor / gana_bracket (pickem / streak / bracket)
-- Registration is platform-level (one shared fct_gana_gamezone__registrations
-- table, game_type=null on the registration event itself per project memory) —
-- the same daily registration count is replicated on all 3 gana game_id rows
-- (confirmed acceptable). Summing `registrations` across game_id for gana will
-- triple-count; consumers must filter to one gana game_id first if they need a
-- platform-level registrations total.
--------------------------------------------------------------------------------

gana_regs_raw as (
    select
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', registered_at) as date) as date_day
    from {{ ref('fct_gana_gamezone__registrations') }}
),

gana_regs as (
    select date_day, count(*) as registrations
    from gana_regs_raw group by 1
),

gana_predictor_ents_raw as (
    select
        user_id,
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', entered_at) as date) as date_day
    from {{ ref('fct_gana_gamezone__predictor_entries') }}
),

gana_predictor_bounds as (
    select min(date_day) as min_date from gana_predictor_ents_raw
),

gana_predictor_spine as (
    select dateadd(day, seq4(), (select min_date from gana_predictor_bounds)) as date_day
    from table(generator(rowcount => 3000))
    where date_day <= current_date()
),

gana_predictor_daily as (
    select date_day, count(*) as total_entries, count(distinct user_id) as unique_entrants
    from gana_predictor_ents_raw group by 1
),

gana_predictor_wau as (
    select s.date_day, count(distinct e.user_id) as wau
    from gana_predictor_spine s
    left join gana_predictor_ents_raw e
        on e.date_day between dateadd(day, -6, s.date_day) and s.date_day
    group by 1
),

gana_predictor_mau as (
    select s.date_day, count(distinct e.user_id) as mau
    from gana_predictor_spine s
    left join gana_predictor_ents_raw e
        on e.date_day between dateadd(day, -27, s.date_day) and s.date_day
    group by 1
),

gana_predictor_first_entries_daily as (
    select first_date as date_day, count(*) as first_entries
    from (
        select user_id, min(date_day) as first_date
        from gana_predictor_ents_raw group by 1
    )
    group by 1
),

game_gana_predictor as (
    select
        s.date_day,
        'gana_predictor' as game_id,
        'Gana Predictor' as game_name,
        'pickem' as game_type,
        'gana' as client_id,
        'gana_gamezone' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        coalesce(r.registrations, 0) as registrations,
        coalesce(d.total_entries, 0) as entries,
        coalesce(d.unique_entrants, 0) as dau,
        coalesce(w.wau, 0) as wau,
        coalesce(m.mau, 0) as mau,
        coalesce(fe.first_entries, 0) as first_entries,
        cast(null as integer) as purchases,
        cast(null as number) as gross_revenue,
        cast(null as integer) as dpu,
        cast(null as integer) as first_purchases,
        cast(null as integer) as wpu,
        cast(null as integer) as mpu
    from gana_predictor_spine s
    left join gana_predictor_daily d on s.date_day = d.date_day
    left join gana_predictor_wau w on s.date_day = w.date_day
    left join gana_predictor_mau m on s.date_day = m.date_day
    left join gana_regs r on s.date_day = r.date_day
    left join gana_predictor_first_entries_daily fe on s.date_day = fe.date_day
),

gana_survivor_ents_raw as (
    select
        user_id,
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', selected_at) as date) as date_day
    from {{ ref('fct_gana_gamezone__survivor_entries') }}
    where selected_country_id is not null
),

gana_survivor_bounds as (
    select min(date_day) as min_date from gana_survivor_ents_raw
),

gana_survivor_spine as (
    select dateadd(day, seq4(), (select min_date from gana_survivor_bounds)) as date_day
    from table(generator(rowcount => 3000))
    where date_day <= current_date()
),

gana_survivor_daily as (
    select date_day, count(*) as total_entries, count(distinct user_id) as unique_entrants
    from gana_survivor_ents_raw group by 1
),

gana_survivor_wau as (
    select s.date_day, count(distinct e.user_id) as wau
    from gana_survivor_spine s
    left join gana_survivor_ents_raw e
        on e.date_day between dateadd(day, -6, s.date_day) and s.date_day
    group by 1
),

gana_survivor_mau as (
    select s.date_day, count(distinct e.user_id) as mau
    from gana_survivor_spine s
    left join gana_survivor_ents_raw e
        on e.date_day between dateadd(day, -27, s.date_day) and s.date_day
    group by 1
),

gana_survivor_first_entries_daily as (
    select first_date as date_day, count(*) as first_entries
    from (
        select user_id, min(date_day) as first_date
        from gana_survivor_ents_raw group by 1
    )
    group by 1
),

game_gana_survivor as (
    select
        s.date_day,
        'gana_survivor' as game_id,
        'Gana Survivor' as game_name,
        'streak' as game_type,
        'gana' as client_id,
        'gana_gamezone' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        coalesce(r.registrations, 0) as registrations,
        coalesce(d.total_entries, 0) as entries,
        coalesce(d.unique_entrants, 0) as dau,
        coalesce(w.wau, 0) as wau,
        coalesce(m.mau, 0) as mau,
        coalesce(fe.first_entries, 0) as first_entries,
        cast(null as integer) as purchases,
        cast(null as number) as gross_revenue,
        cast(null as integer) as dpu,
        cast(null as integer) as first_purchases,
        cast(null as integer) as wpu,
        cast(null as integer) as mpu
    from gana_survivor_spine s
    left join gana_survivor_daily d on s.date_day = d.date_day
    left join gana_survivor_wau w on s.date_day = w.date_day
    left join gana_survivor_mau m on s.date_day = m.date_day
    left join gana_regs r on s.date_day = r.date_day
    left join gana_survivor_first_entries_daily fe on s.date_day = fe.date_day
),

gana_bracket_ents_raw as (
    select
        user_id,
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', entered_at) as date) as date_day
    from {{ ref('fct_gana_gamezone__bracket_entries') }}
),

gana_bracket_bounds as (
    select min(date_day) as min_date from gana_bracket_ents_raw
),

gana_bracket_spine as (
    select dateadd(day, seq4(), (select min_date from gana_bracket_bounds)) as date_day
    from table(generator(rowcount => 3000))
    where date_day <= current_date()
),

gana_bracket_daily as (
    select date_day, count(*) as total_entries, count(distinct user_id) as unique_entrants
    from gana_bracket_ents_raw group by 1
),

gana_bracket_wau as (
    select s.date_day, count(distinct e.user_id) as wau
    from gana_bracket_spine s
    left join gana_bracket_ents_raw e
        on e.date_day between dateadd(day, -6, s.date_day) and s.date_day
    group by 1
),

gana_bracket_mau as (
    select s.date_day, count(distinct e.user_id) as mau
    from gana_bracket_spine s
    left join gana_bracket_ents_raw e
        on e.date_day between dateadd(day, -27, s.date_day) and s.date_day
    group by 1
),

gana_bracket_first_entries_daily as (
    select first_date as date_day, count(*) as first_entries
    from (
        select user_id, min(date_day) as first_date
        from gana_bracket_ents_raw group by 1
    )
    group by 1
),

game_gana_bracket as (
    select
        s.date_day,
        'gana_bracket' as game_id,
        'Gana Bracket' as game_name,
        'bracket' as game_type,
        'gana' as client_id,
        'gana_gamezone' as source_schema,
        '{{ target.database }}' as source_database,
        cast(null as varchar) as tenant_name,
        coalesce(r.registrations, 0) as registrations,
        coalesce(d.total_entries, 0) as entries,
        coalesce(d.unique_entrants, 0) as dau,
        coalesce(w.wau, 0) as wau,
        coalesce(m.mau, 0) as mau,
        coalesce(fe.first_entries, 0) as first_entries,
        cast(null as integer) as purchases,
        cast(null as number) as gross_revenue,
        cast(null as integer) as dpu,
        cast(null as integer) as first_purchases,
        cast(null as integer) as wpu,
        cast(null as integer) as mpu
    from gana_bracket_spine s
    left join gana_bracket_daily d on s.date_day = d.date_day
    left join gana_bracket_wau w on s.date_day = w.date_day
    left join gana_bracket_mau m on s.date_day = m.date_day
    left join gana_regs r on s.date_day = r.date_day
    left join gana_bracket_first_entries_daily fe on s.date_day = fe.date_day
)

select * from game_bet365_overunder
union all
select * from game_bet99_picks
union all
select * from game_betway_picks
union all
select * from game_saracen_pickem
union all
select * from game_saracen_bracket
union all
select * from game_bet99_bracket
union all
select * from game_bet365_uf
union all
select * from game_cfl_fantasy
union all
select * from game_opap_spintowin
union all
select * from game_gana_predictor
union all
select * from game_gana_survivor
union all
select * from game_gana_bracket
