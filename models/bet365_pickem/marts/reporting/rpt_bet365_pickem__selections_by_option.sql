with

contests as (

    select *
    from {{ ref('dim_bet365_pickem__contests') }}

),

questions as (

    select *
    from {{ ref('dim_bet365_pickem__questions') }}

),

options as (

    select *
    from {{ ref('dim_bet365_pickem__options') }}

),

entries as (

    select *
    from {{ ref('fct_bet365_pickem__entries') }}

),

selections as (

    select *
    from {{ ref('fct_bet365_pickem__selections') }}

),

-- the jurisdictions each contest was entered from
contest_tenants as (

    select
        contest_id,
        tenant_id,
        max(tenant_name) as tenant_name,
        max(country_code) as country_code,
        max(state_code) as state_code
    from entries
    group by contest_id, tenant_id

),

-- Every active option of every active question, for each tenant that entered
-- the contest (so unpicked options show 0), plus any option actually picked
-- (covers picks on options or questions removed later).
option_rows as (

    select
        questions.contest_id,
        questions.question_id,
        options.option_id,
        contest_tenants.tenant_id
    from questions
    inner join options
        on questions.question_id = options.question_id
    inner join contest_tenants
        on questions.contest_id = contest_tenants.contest_id
    where questions.is_active
        and options.is_active

    union

    select
        contest_id,
        question_id,
        option_id,
        tenant_id
    from selections

),

option_selections as (

    select
        contest_id,
        question_id,
        option_id,
        tenant_id,
        count(*) as selections,
        avg(price_decimal) as avg_price_decimal
    from selections
    group by contest_id, question_id, option_id, tenant_id

)

select
    option_rows.contest_id,
    option_rows.question_id,
    option_rows.option_id,

    contests.client_id,
    option_rows.tenant_id,
    contest_tenants.tenant_name,
    contests.game_type,

    contest_tenants.country_code,
    contest_tenants.state_code,
    contests.contest_name,
    contests.contest_status,
    contests.locks_at,
    questions.fixture_id,
    questions.fixture_name,
    questions.is_main_event,
    questions.fixture_position,
    questions.question_type,
    questions.question_text,
    questions.question_position,
    options.option_key,
    options.option_text,
    options.option_position,
    options.competitor_id,
    options.competitor_name,

    coalesce(option_selections.selections, 0) as selections,
    round(
        coalesce(option_selections.selections, 0)
        / nullif(sum(coalesce(option_selections.selections, 0)) over (
            partition by option_rows.contest_id, option_rows.question_id, option_rows.tenant_id
        ), 0),
        4
    ) as selection_share,
    round(option_selections.avg_price_decimal, 3) as avg_price_decimal

from option_rows
left join contests
    on option_rows.contest_id = contests.contest_id
left join questions
    on option_rows.question_id = questions.question_id
left join options
    on option_rows.option_id = options.option_id
left join contest_tenants
    on option_rows.contest_id = contest_tenants.contest_id
    and option_rows.tenant_id = contest_tenants.tenant_id
left join option_selections
    on option_rows.contest_id = option_selections.contest_id
    and option_rows.question_id = option_selections.question_id
    and option_rows.option_id = option_selections.option_id
    and option_rows.tenant_id = option_selections.tenant_id
