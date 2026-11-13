with

questions as (

    select *
    from {{ ref('stg_bet365_pickem__questions') }}

),

contest_fixtures as (

    select *
    from {{ ref('stg_bet365_pickem__contest_fixtures') }}

),

options as (

    select *
    from {{ ref('int_bet365_pickem__options_joined_to_competitors') }}

),

-- The source has no fixture name: build it from the competitors on the
-- fixture's options, in the order they first appear ('A vs B').
fixture_competitors as (

    select
        questions.contest_fixture_id,
        options.competitor_name,
        min(questions.question_position) as first_question_position,
        min(options.option_position) as first_option_position
    from questions
    inner join options
        on questions.question_id = options.question_id
    where options.competitor_name is not null
        and questions.is_active
        and options.is_active
    group by questions.contest_fixture_id, options.competitor_name

),

fixture_names as (

    select
        contest_fixture_id,
        listagg(competitor_name, ' vs ') within group (
            order by first_question_position, first_option_position, competitor_name
        ) as fixture_name
    from fixture_competitors
    group by contest_fixture_id

)

select
    questions.question_id,
    questions.contest_fixture_id,
    contest_fixtures.contest_id,
    contest_fixtures.fixture_id,

    questions.client_id,
    questions.game_type,

    fixture_names.fixture_name,
    questions.definition_key,
    questions.question_type,
    questions.copy_key,
    questions.copy_override,
    questions.question_text,
    questions.question_params,

    contest_fixtures.fixture_position,
    questions.question_position,

    contest_fixtures.is_main_event,
    questions.is_active,

    contest_fixtures.lock_at,
    questions.created_at,
    questions.removed_at

from questions
left join contest_fixtures
    on questions.contest_fixture_id = contest_fixtures.contest_fixture_id
left join fixture_names
    on questions.contest_fixture_id = fixture_names.contest_fixture_id
