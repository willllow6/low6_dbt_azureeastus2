with

picks as (

    select *
    from {{ ref('stg_bet365_overunder__picks') }}

),

entries as (

    select *
    from {{ ref('bet365_overunder__entries') }}

),

joined as (

    select
        picks.pick_id,
        picks.entry_id,
        picks.prop_id,
        entries.user_id,
        entries.gaming_id,

        picks.picked_option,
        picks.player_name,
        picks.player_position,
        picks.team_abbr,
        picks.opponent_abbr,
        picks.fixture_name,
        picks.market_name,
        picks.market_handicap,
        picks.odds_fractional,
        picks.odds_decimal,
        picks.odds_american,
        picks.is_correct,

        entries.country,
        entries.state_province,
        entries.segment_group,
        entries.contest_date_et,
        entries.entry_date,
        entries.entry_date_et,

        picks.picked_at,
        picks.picked_at_et
    from picks
    inner join entries
        on picks.entry_id = entries.entry_id

)

select * from joined