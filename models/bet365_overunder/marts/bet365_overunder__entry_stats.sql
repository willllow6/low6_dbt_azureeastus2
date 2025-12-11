with

entries as (

    select *
    from {{ ref('bet365_overunder__entries') }}

),

entry_stats as (

    select
        entry_date,
        entry_date_et,
        entry_hour,
        entry_hour_et,
        entry_type,
        country,
        state_province,
        segment_group,
        sport_combination,
        contest_date_et,
        entered_picks,
        currency_code,
        count(*) as entries,
        sum(case when entry_type = 'First Entry' then 1 else 0 end) as first_entries,
        sum(case when entry_type = 'Repeat Entry' then 1 else 0 end) as repeat_entries,
        sum(potential_prize_amount) as potential_prize_amount,
        sum(prize_amount) as prize_amount,
        sum(case when is_winner then 1 else 0 end) as winning_entries,
        sum(case when is_first_win then 1 else 0 end) as first_wins,
        sum(nba_picks) as total_nba_picks,
        sum(nfl_picks) as total_nfl_picks,
        sum(nhl_picks) as total_nhl_picks
    from entries
    group by 1,2,3,4,5,6,7,8,9,10,11,12

)

select * from entry_stats