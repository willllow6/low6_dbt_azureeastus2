with

contests as (

    select *
    from {{ ref('stg_bet365_pickem__contests') }}

),

contest_fixtures as (

    select *
    from {{ ref('stg_bet365_pickem__contest_fixtures') }}

),

users as (

    select *
    from {{ ref('stg_bet365_pickem__users') }}

),

entries as (

    select *
    from {{ ref('stg_bet365_pickem__entries') }}

),

launches as (

    select *
    from {{ ref('int_bet365_pickem__launches_unioned') }}

),

entry_selections as (

    select *
    from {{ ref('int_bet365_pickem__selections_aggregated_to_entries') }}

),

-- A contest is open for entries from when it opens until its first fixture
-- locks. Cancelled and never-opened contests have no window.
contest_windows as (

    select
        contests.contest_id,
        coalesce(contests.opened_at, contests.opens_at) as window_starts_at,
        min(contest_fixtures.lock_at) as window_ends_at
    from contests
    inner join contest_fixtures
        on contests.contest_id = contest_fixtures.contest_id
    where contests.cancelled_at is null
        and coalesce(contests.opened_at, contests.opens_at) is not null
    group by contests.contest_id, coalesce(contests.opened_at, contests.opens_at)

),

-- An entry row (saved or submitted) always proves the user was there. Otherwise
-- any known launch inside the window counts: captured launches (from
-- 2026-11-13) plus the first and latest launch from the players table.
eligibility_rows as (

    select
        entries.user_id,
        entries.contest_id,
        'entry' as eligibility_source,
        0 as source_priority
    from entries
    inner join contest_windows
        on entries.contest_id = contest_windows.contest_id

    union all

    select
        launches.user_id,
        contest_windows.contest_id,
        launches.launch_source as eligibility_source,
        case launches.launch_source
            when 'first_launch' then 1
            when 'captured_launch' then 2
            else 3
        end as source_priority
    from launches
    inner join contest_windows
        on launches.launched_at >= contest_windows.window_starts_at
        and launches.launched_at < contest_windows.window_ends_at

),

eligible as (

    select
        user_id,
        contest_id,
        eligibility_source
    from eligibility_rows
    qualify row_number() over (
        partition by user_id, contest_id
        order by source_priority
    ) = 1

)

select
    {{ dbt_utils.generate_surrogate_key(['eligible.user_id', 'eligible.contest_id']) }} as contest_funnel_id,
    eligible.user_id,
    eligible.contest_id,
    entries.entry_id,

    users.client_id,
    -- the jurisdiction the entry was made from, or the player's own when they never started
    coalesce(entries.tenant_id, users.tenant_id) as tenant_id,
    coalesce(entries.tenant_name, users.tenant_name) as tenant_name,
    users.game_type,

    coalesce(entries.country_code, users.country_code) as country_code,
    coalesce(entries.state_code, users.state_code) as state_code,
    eligible.eligibility_source,
    case
        when entries.entry_id is null then 'launched_not_started'
        when entries.refused_at is not null then 'refused'
        when entries.is_valid then 'submitted'
        else 'saved_not_submitted'
    end as funnel_stage,

    coalesce(entry_selections.selections, 0) as picks_saved,

    entries.entry_id is not null as has_started,
    coalesce(entries.is_valid, false) as has_submitted,

    entries.created_at as started_at,
    entries.entered_at as submitted_at

from eligible
inner join users
    on eligible.user_id = users.user_id
left join entries
    on eligible.user_id = entries.user_id
    and eligible.contest_id = entries.contest_id
left join entry_selections
    on entries.entry_id = entry_selections.entry_id
