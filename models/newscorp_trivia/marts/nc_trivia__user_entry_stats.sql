with

user_entries as (

    select * from {{ ref('nc_trivia__user_entries') }}

),

agg_user_entries as (

    select
        contest_title,
        contest_status,
        contest_start_date_aet,
        entered_date_aet,
        entered_day_aet,
        entered_hour_aet,
        user_entry_type,
        count(*) as entry_count,
        sum(case when user_entry_number = 1 then 1 else 0 end) as player_count
    from user_entries
    group by 1, 2, 3, 4, 5, 6, 7

)

select * from agg_user_entries
