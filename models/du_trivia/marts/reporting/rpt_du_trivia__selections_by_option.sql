with

questions as (

    select *
    from {{ ref('dim_du_trivia__questions') }}

),

options as (

    select *
    from {{ ref('dim_du_trivia__options') }}

),

contests as (

    select *
    from {{ ref('dim_du_trivia__contests') }}

),

selections as (

    select *
    from {{ ref('fct_du_trivia__selections') }}

),

-- Every option of every question, plus one null-option row for questions where
-- a user made a selection without choosing an option (e.g. timed out).
option_rows as (

    select
        questions.contest_id,
        questions.question_id,
        options.option_id
    from questions
    inner join options
        on questions.question_id = options.question_id

    union

    select
        contest_id,
        question_id,
        null as option_id
    from selections
    where option_id is null

),

option_selections as (

    select
        contest_id,
        question_id,
        option_id,
        count(*) as selections,
        count(distinct user_id) as unique_users,
        avg(elapsed_seconds) as avg_elapsed_seconds
    from selections
    group by contest_id, question_id, option_id

)

select
    option_rows.contest_id,
    option_rows.question_id,
    option_rows.option_id,

    questions.client_id,
    questions.tenant_id,
    questions.tenant_name,
    questions.game_type,

    contests.contest_name,
    contests.contest_date,
    contests.starts_at,
    contests.starts_at_gst,
    questions.question_title,
    questions.question_type,
    questions.question_points,
    options.option_title,
    options.is_correct as is_correct_option,

    coalesce(option_selections.selections, 0) as selections,
    coalesce(option_selections.unique_users, 0) as unique_users,
    round(
        coalesce(option_selections.selections, 0)
        / nullif(sum(coalesce(option_selections.selections, 0)) over (
            partition by option_rows.contest_id, option_rows.question_id
        ), 0),
        4
    ) as selection_share,
    round(option_selections.avg_elapsed_seconds, 2) as avg_elapsed_seconds

from option_rows
inner join questions
    on option_rows.question_id = questions.question_id
    and option_rows.contest_id = questions.contest_id
left join options
    on option_rows.option_id = options.option_id
left join contests
    on option_rows.contest_id = contests.contest_id
left join option_selections
    on option_rows.contest_id = option_selections.contest_id
    and option_rows.question_id = option_selections.question_id
    and equal_null(option_rows.option_id, option_selections.option_id)
