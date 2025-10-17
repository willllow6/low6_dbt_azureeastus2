with

selections as (

    select *
    from {{ ref('stg_betway_picks__user_selections') }}

),

questions as (

    select *
    from {{ ref('stg_betway_picks__questions') }}

),

options as (

    select *
    from {{ ref('stg_betway_picks__options') }}

),

users as (

    select *
    from {{ ref('stg_betway_picks__users') }}

),

joined as (

    select
        selections.selection_id,
        selections.user_id,
        users.sso_user_id,
        selections.contest_id,
        selections.question_id,
        selections.option_id,

        users.username,

        questions.question_type,
        questions.question_text,

        options.option_text as selected_option_text,
        options.is_correct as is_selection_correct,

        selections.tiebreaker_prediction,
        
        selections.created_date,
        selections.created_date_et,
        selections.created_at,
        selections.created_at_et

    from selections 
    left join users
        on selections.user_id = users.user_id
    left join questions
        on selections.question_id = questions.question_id
        and selections.contest_id = questions.contest_id
    left join options 
        on selections.option_id = options.option_id
        and selections.question_id = options.question_id

)

select * from joined