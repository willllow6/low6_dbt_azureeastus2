with

user_selections as (

    select *
    from {{ ref('nc_trivia__user_selections') }}

),

agg_user_selections as (

    select
        contest_title,
        contest_status,
        question_title,
        option_title,
        count(*) as user_selection_count,
        sum(case when is_correct then 1 else 0 end) as correct_selection_count,
        sum(elapsed_time_seconds) as total_elapsed_time_seconds
    from user_selections
    group by 1, 2, 3, 4

)

select * from agg_user_selections