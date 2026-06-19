with

entries as (

    select *
    from {{ ref('mart_saracen__entries') }}

),

user_active_days as (

    select
        sso_user_id,
        entry_date_et as date_day,
        count(*) as total_entries
    from entries
    group by 1, 2

)

select * from user_active_days
