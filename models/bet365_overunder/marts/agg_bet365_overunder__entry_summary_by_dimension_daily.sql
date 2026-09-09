with

entries as (

    select *
    from {{ ref('fct_bet365_overunder__entries') }}

),

entry_summary_by_dimension as (

    select
        entry_date_et,
        client_id,
        tenant_id,
        country,
        state_province,
        segment_group,
        entered_picks,
        count(*) as entries,
        sum(case when is_winner then 1 else 0 end) as winning_entries
    from entries
    group by 1,2,3,4,5,6,7

)

select * from entry_summary_by_dimension
