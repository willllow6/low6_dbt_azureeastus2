with

selections as (

    select *
    from {{ ref('mart_saracen_picks__selections') }}

),

contests as (

    select *
    from {{ ref('dim_saracen_picks__contests') }}

),

joined as (

    select
        selections.contest_id,
        contests.contest_name,
        selections.question_id,
        selections.question_text,
        selections.option_id,
        selections.selected_option_text,
        selections.is_selection_correct
    from selections
    left join contests
        on selections.contest_id = contests.contest_id

),

question_stats as (

    select
        contest_id,
        contest_name,
        question_id,
        question_text,
        option_id,
        selected_option_text,
        count(*) as total_selections,
        sum(case when is_selection_correct then 1 else 0 end) as correct_selections,
        div0(
            sum(case when is_selection_correct then 1 else 0 end),
            count(case when is_selection_correct is not null then 1 end)
        ) as accuracy_rate
    from joined
    group by 1, 2, 3, 4, 5, 6

)

select * from question_stats
