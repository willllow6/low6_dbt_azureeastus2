with

entries as (

    select *
    from {{ ref('fct_bet365_overunder__entries') }}

),

sport_combination_summary as (

    select
        contest_date_et,
        entry_date_et,
        client_id,
        tenant_id,
        country,
        sport_combination,
        count(*) as entries,
        avg(entered_picks) as avg_picks_per_entry
    from entries
    group by 1,2,3,4,5,6

)

select * from sport_combination_summary
