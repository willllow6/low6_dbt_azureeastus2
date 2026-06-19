with

user_identity as (

    select *
    from {{ ref('int_saracen__user_identity_spine') }}

),

registrations as (

    select
        cast(
            least(
                coalesce(pickem_created_at_et, bracket_created_at_et),
                coalesce(bracket_created_at_et, pickem_created_at_et)
            ) as date
        ) as date_day,
        count(distinct sso_user_id) as registrations
    from user_identity
    group by 1

),

entries as (

    select
        entry_date_et as date_day,
        count(distinct sso_user_id) as dau,
        count(*) as total_entries,
        sum(case when game_type = 'pickem' then 1 else 0 end) as pickem_entries,
        sum(case when game_type = 'bracket' then 1 else 0 end) as bracket_entries
    from {{ ref('mart_saracen__entries') }}
    group by 1

),

combined as (

    select
        coalesce(registrations.date_day, entries.date_day) as date_day,
        coalesce(registrations.registrations, 0) as registrations,
        coalesce(entries.dau, 0) as dau,
        coalesce(entries.total_entries, 0) as total_entries,
        coalesce(entries.pickem_entries, 0) as pickem_entries,
        coalesce(entries.bracket_entries, 0) as bracket_entries
    from registrations
    full outer join entries
        on registrations.date_day = entries.date_day

)

select * from combined
