with

contests as (

    select *
    from {{ ref('stg_bet365_pickem__contests') }}

),

contest_fixtures as (

    select *
    from {{ ref('stg_bet365_pickem__contest_fixtures') }}

),

questions as (

    select *
    from {{ ref('stg_bet365_pickem__questions') }}

),

fixture_question_counts as (

    select
        contest_fixture_id,
        count_if(is_active) as questions
    from questions
    group by contest_fixture_id

),

contest_fixture_metrics as (

    select
        contest_fixtures.contest_id,
        count(*) as fixtures,
        coalesce(sum(fixture_question_counts.questions), 0) as questions,
        min(contest_fixtures.lock_at) as locks_at
    from contest_fixtures
    left join fixture_question_counts
        on contest_fixtures.contest_fixture_id = fixture_question_counts.contest_fixture_id
    group by contest_fixtures.contest_id

)

select
    contests.contest_id,
    contests.event_id,

    contests.client_id,
    -- contests are not tied to a jurisdiction: tenants come from entries
    cast(null as varchar) as tenant_id,
    cast(null as varchar) as tenant_name,
    contests.game_type,

    contests.contest_name,
    contests.contest_status,
    contests.ruleset,
    contests.event_class,
    contests.revision,

    coalesce(contest_fixture_metrics.fixtures, 0) as fixtures,
    coalesce(contest_fixture_metrics.questions, 0) as questions,

    contests.is_active,

    coalesce(contests.opened_at, contests.opens_at) as starts_at,
    contest_fixture_metrics.locks_at,
    coalesce(contests.closed_at, contests.cancelled_at) as ends_at,
    convert_timezone('UTC', '{{ var("local_timezone") }}', coalesce(contests.opened_at, contests.opens_at)) as starts_at_et,
    convert_timezone('UTC', '{{ var("local_timezone") }}', contest_fixture_metrics.locks_at) as locks_at_et,

    contests.created_at,
    contests.published_at,
    contests.opens_at,
    contests.opened_at,
    contests.settled_at,
    contests.hold_started_at,
    contests.hold_ends_at,
    contests.paying_at,
    contests.paid_at,
    contests.closed_at,
    contests.cancelled_at

from contests
left join contest_fixture_metrics
    on contests.contest_id = contest_fixture_metrics.contest_id
