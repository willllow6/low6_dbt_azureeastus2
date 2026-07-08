with

selections as (

    select *
    from {{ ref('stg_nc_trivia__user_selections') }}

),

contests as (

    select *
    from {{ ref('stg_nc_trivia__contests') }}

),

questions as (

    select *
    from {{ ref('stg_nc_trivia__questions') }}

),

options as (

    select *
    from {{ ref('stg_nc_trivia__options') }}

),

users as (

    select *
    from {{ ref('stg_nc_trivia__users') }}

),

joined as (

    select
        selections.contest_id,
        selections.user_id,
        selections.question_id,
        selections.option_id,

        users.service_user_id,
        users.sso_user_id,
        users.username,
        users.nickname,

        selections.elapsed_time_seconds,
        selections.selection_points,
        selections.is_correct,
        selections.started_at_aet,
        selections.selected_at_aet,
        
        contests.contest_title,
        contests.contest_status,
        contests.contest_start_date_aet,

        questions.question_title,
        -- questions.question_type,
        questions.question_duration,
        questions.question_maximum_points,

        options.option_title
    
    from selections
    left join users
        on selections.user_id = users.user_id
    left join contests
        on selections.contest_id = contests.contest_id
    left join questions 
        on selections.question_id = questions.question_id
    left join options
        on selections.option_id = options.option_id

)

select * from joined