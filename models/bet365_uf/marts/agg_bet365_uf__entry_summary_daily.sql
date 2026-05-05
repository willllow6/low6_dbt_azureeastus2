with

entries as (
    select *
    from {{ ref('mart_bet365_uf__entries') }}
    where is_tester = false
),

agg_entries as (

    select
        tenant_name,
        competition_name,
        is_international_competition,
        game_week_name,
        game_week_description,
        game_week_start_date,
        contest_name,
        contest_start_date,
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', entered_at) as date) as entry_date,
        is_completed,
        entry_number = 1 as is_first_entry,
        count(*) as entries,
        sum(case when entry_number = 1 then 1 else 0 end) as first_entries,
        sum(case when tenant_entry_number = 1 then 1 else 0 end) as first_tenant_entries
    from entries
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11

)

select * from agg_entries
