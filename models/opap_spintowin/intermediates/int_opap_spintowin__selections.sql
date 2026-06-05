with

picks as (

    select *
    from {{ ref('stg_opap_spintowin__picks') }}

),

entries as (

    select
        entry_id,
        user_id,
        contest_id,
        entered_at as created_at,
        updated_at
    from {{ ref('stg_opap_spintowin__entries') }}

),

reel_options as (

    select
        reel_option_id,
        reel_id,
        contest_fixture_id,
        external_market_id,
        external_outcome_id,
        market_name,
        outcome_name
    from {{ ref('stg_opap_spintowin__reel_options') }}

),

contest_reels as (

    select
        reel_id,
        contest_id,
        reel_number
    from {{ ref('stg_opap_spintowin__contest_reels') }}

),

contest_fixtures as (

    select
        contest_fixture_id,
        external_fixture_id,
        fixture_name
    from {{ ref('stg_opap_spintowin__contest_fixtures') }}

),

joined as (

    select
        p.pick_id                       as selection_id,
        p.entry_id,
        e.user_id,
        e.contest_id,
        cr.reel_id,

        ---------- canonical fields
        'opap'                          as client_id,
        'opap'                          as tenant_id,
        'spin_to_win'                   as game_type,

        ---------- selection details
        cf.external_fixture_id          as event_id,
        ro.external_market_id           as market_id,
        ro.external_outcome_id          as outcome_id,
        cf.fixture_name                 as event_name,
        ro.market_name                  as market_name,
        cr.reel_number                  as selection_sequence,
        ro.outcome_name                 as selection_value,
        p.selection_status,

        ---------- timestamps
        e.created_at,
        e.updated_at

    from picks p
    inner join entries e
        on p.entry_id = e.entry_id
    inner join reel_options ro
        on p.reel_option_id = ro.reel_option_id
    inner join contest_reels cr
        on ro.reel_id = cr.reel_id
    left join contest_fixtures cf
        on ro.contest_fixture_id = cf.contest_fixture_id

)

select * from joined
