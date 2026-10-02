with

contests as (

    select *
    from {{ ref('stg_oilers_picks__contests') }}

),

questions as (

    select *
    from {{ ref('stg_oilers_picks__questions') }}

),

contest_questions as (

    select
        contest_id,
        count(*) as questions,
        max(case when is_tiebreak_question then tiebreak_outcome end) as tiebreak_outcome
    from questions
    where is_active
    group by contest_id

)

select
    contests.contest_id,

    contests.client_id,
    contests.tenant_id,
    contests.tenant_name,
    contests.game_type,

    contests.contest_name,
    contests.contest_status,
    contests.prize_text,

    coalesce(contest_questions.questions, 0) as questions,
    contest_questions.tiebreak_outcome,

    contests.is_active,

    cast(
        convert_timezone('UTC', '{{ var("local_timezone") }}', contests.starts_at) as date
    ) as contest_start_date_et,

    contests.starts_at,
    convert_timezone('UTC', '{{ var("local_timezone") }}', contests.starts_at) as starts_at_et,
    -- the source has no contest end time
    cast(null as timestamp_ntz) as ends_at,
    contests.selections_open_at,
    contests.created_at,
    contests.updated_at

from contests
left join contest_questions
    on contests.contest_id = contest_questions.contest_id
