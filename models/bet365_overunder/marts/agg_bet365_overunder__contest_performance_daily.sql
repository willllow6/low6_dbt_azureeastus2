with

entries as (

    select *
    from {{ ref('fct_bet365_overunder__entries') }}

),

contest_performance as (

    select
        contest_date_et,
        entry_date_et,
        client_id,
        tenant_id,
        country,
        currency_code,
        count(*) as entries,
        sum(case when entry_type = 'First Entry' then 1 else 0 end) as first_entries,
        sum(case when entry_type = 'Repeat Entry' then 1 else 0 end) as repeat_entries,
        sum(case when is_winner then 1 else 0 end) as winning_entries,
        sum(prize_amount) as prize_amount
    from entries
    group by 1,2,3,4,5,6

)

select * from contest_performance
