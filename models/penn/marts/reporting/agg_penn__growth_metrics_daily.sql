with

date_spine as (

    select dateadd('day', seq4(), '{{ var("penn_start_date") }}'::date) as date_day
    from table(generator(rowcount => 1000))
    where dateadd('day', seq4(), '{{ var("penn_start_date") }}'::date) <= sysdate()

),

tenants as (

    select
        tenant_id,
        tenant_name
    from {{ ref('dim_penn__tenants') }}

),

date_tenants as (

    select
        date_spine.date_day,
        tenants.tenant_id,
        tenants.tenant_name
    from date_spine
    cross join tenants

),

registrations as (

    -- Registrations are not tenant-scoped; summed by date_day only so the total
    -- is broadcast to all tenant rows on that date (same as tipman pattern).
    select
        date_day,
        sum(new_registrations) as new_registrations
    from {{ ref('agg_penn__registration_metrics_daily') }}
    group by date_day

),

user_activity as (

    select *
    from {{ ref('agg_penn__user_activity_daily') }}

),

entries as (

    select *
    from {{ ref('agg_penn__entry_metrics_daily') }}

),

players_revealed as (

    select
        cast(revealed_at_et as date) as date_day,
        tenant_id,
        count(selection_id) as players_revealed
    from {{ ref('fct_penn__selections') }}
    where status = 'revealed'
        or revealed_at is not null
    group by
        cast(revealed_at_et as date),
        tenant_id

),

awards as (

    select
        delivered_date_et as date_day,
        tenant_id,
        count(award_id) as total_awards,
        sum(prize_amount) as total_prize_amount
    from {{ ref('fct_penn__awards') }}
    where award_status = 'delivered'
    group by
        delivered_date_et,
        tenant_id

),

conversion as (

    -- Registration-to-entry rate is not tenant-scoped (registrations have no tenant);
    -- joined by date_day only so the cohort metrics broadcast across all tenant rows.
    select
        date_day,
        registered,
        activated,
        activation_rate as registration_to_entry_rate
    from {{ ref('agg_penn__conversion_funnel_daily') }}

),

joined as (

    select
        dt.date_day,
        'penn' as client_id,
        dt.tenant_id,
        dt.tenant_name,
        'squads' as game_type,

        coalesce(r.new_registrations, 0) as new_registrations,

        coalesce(ua.dau, 0) as dau,
        coalesce(ua.returning_users, 0) as returning_users,

        coalesce(e.total_entries, 0) as total_entries,
        coalesce(e.unique_entrants, 0) as unique_entrants,
        coalesce(e.first_time_entrants, 0) as first_time_entrants,

        coalesce(pr.players_revealed, 0) as players_revealed,

        coalesce(a.total_awards, 0) as total_awards,
        coalesce(a.total_prize_amount, 0) as total_prize_amount,

        coalesce(c.registered, 0) as registered,
        coalesce(c.activated, 0) as ever_entered,
        c.registration_to_entry_rate

    from date_tenants as dt
    left join registrations as r
        on dt.date_day = r.date_day
    left join user_activity as ua
        on dt.date_day = ua.date_day
        and dt.tenant_id = ua.tenant_id
    left join entries as e
        on dt.date_day = e.date_day
        and dt.tenant_id = e.tenant_id
    left join players_revealed as pr
        on dt.date_day = pr.date_day
        and dt.tenant_id = pr.tenant_id
    left join awards as a
        on dt.date_day = a.date_day
        and dt.tenant_id = a.tenant_id
    left join conversion as c
        on dt.date_day = c.date_day

)

select * from joined
