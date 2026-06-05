with

contests as (

    select *
    from {{ ref('dim_opap_spintowin__contests') }}

),

entries as (

    select
        contest_id,
        entry_date_et,
        count(*) as contest_entries
    from {{ ref('fct_opap_spintowin__entries') }}
    group by 1, 2

),

contests_daily as (

    select
        e.entry_date_et                             as date_day,
        c.client_id,
        c.tenant_id,
        c.tenant_name,
        c.game_type,
        c.contest_id,
        c.contest_status,
        e.contest_entries
    from entries e
    inner join contests c
        on e.contest_id = c.contest_id

),

aggregated as (

    select
        date_day,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        count(distinct case when contest_status in ('upcoming', 'draft')             then contest_id end) as contests_scheduled,
        count(distinct case when contest_status in ('open', 'in_play', 'active')     then contest_id end) as contests_live,
        count(distinct case when contest_status in ('closed', 'settled', 'complete') then contest_id end) as contests_completed,
        round(avg(contest_entries), 2)             as avg_entries_per_contest
    from contests_daily
    group by 1, 2, 3, 4, 5

)

select * from aggregated
