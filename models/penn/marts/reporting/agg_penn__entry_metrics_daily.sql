with

entries as (

    select
        entry_id,
        user_id,
        contest_id,
        client_id,
        tenant_id,
        game_type,
        user_entry_number,
        entry_date_et as entry_date
    from {{ ref('fct_penn__entries') }}

),

tenants as (

    select
        tenant_id,
        tenant_name
    from {{ ref('dim_penn__tenants') }}

),

daily as (

    select
        entries.entry_date as date_day,
        entries.client_id,
        entries.tenant_id,
        tenants.tenant_name,
        entries.game_type,
        count(distinct entries.contest_id) as contests_active,
        count(entries.entry_id) as total_entries,
        count(distinct entries.user_id) as unique_entrants,
        count(distinct case
            when entries.user_entry_number = 1
            then entries.user_id
        end) as first_time_entrants
    from entries
    left join tenants
        on entries.tenant_id = tenants.tenant_id
    group by
        entries.entry_date,
        entries.client_id,
        entries.tenant_id,
        tenants.tenant_name,
        entries.game_type

)

select * from daily
