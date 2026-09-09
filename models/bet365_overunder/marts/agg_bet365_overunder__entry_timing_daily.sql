with

entries as (

    select *
    from {{ ref('fct_bet365_overunder__entries') }}

),

entry_timing as (

    select
        entry_date_et,
        client_id,
        tenant_id,
        country,
        entry_hour_et,
        count(*) as entries
    from entries
    group by 1,2,3,4,5

)

select * from entry_timing
