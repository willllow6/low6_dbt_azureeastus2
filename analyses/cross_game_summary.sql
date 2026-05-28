-- ============================================================
-- Entries, Active Users & MAU by Client and Game
-- Period: 2025-07-01 to current date
-- Snowflake: low6_azureeastus2
-- ============================================================

with bet365_overunder_entries as (

    -- Mart uses INNER JOIN on users; entries without a matching user record
    -- are excluded. Source grain: one row per entry.
    select
        cast(user_id as varchar)  as user_id,
        'bet365'                  as client_id,
        'pickem'                  as game_type,
        date_trunc('month', entered_at::date) as entry_month
    from low6_azureeastus2.analytics_bet365_overunder.bet365_overunder__entries
    where entered_at::date >= '2025-07-01'
      and entered_at::date <= current_date()

),

bet365_uf_entries as (

    -- Bet365 Ultimate Fan (fantasy). Canonical client_id and game_type in model.
    select
        cast(user_id as varchar)  as user_id,
        client_id,
        game_type,
        date_trunc('month', entered_at::date) as entry_month
    from low6_azureeastus2.analytics_bet365_uf.fct_bet365_uf__entries
    where entered_at::date >= '2025-07-01'
      and entered_at::date <= current_date()

),

bet99_picks_entries as (

    select
        cast(user_id as varchar)  as user_id,
        'bet99'                   as client_id,
        'pickem'                  as game_type,
        date_trunc('month', entered_at::date) as entry_month
    from low6_azureeastus2.analytics_bet99_picks.bet99_picks__entries
    where entered_at::date >= '2025-07-01'
      and entered_at::date <= current_date()

),

betway_picks_entries as (

    select
        cast(user_id as varchar)  as user_id,
        'betway'                  as client_id,
        'pickem'                  as game_type,
        date_trunc('month', entered_at::date) as entry_month
    from low6_azureeastus2.analytics_betway_picks.betway_picks__entries
    where entered_at::date >= '2025-07-01'
      and entered_at::date <= current_date()

),

elf_entries as (

    -- ELF CollectYourElf: one row per user per challenge. created_at is when
    -- the user first engaged with the challenge (entry equivalent).
    select
        cast(user_id as varchar)  as user_id,
        'elf'                     as client_id,
        'pickem'                  as game_type,
        date_trunc('month', created_at::date) as entry_month
    from low6_azureeastus2.analytics_elf_collectyourelf.fct_elf_collectyourelf__user_challenge_progress
    where created_at::date >= '2025-07-01'
      and created_at::date <= current_date()

),

fanstake_entries as (

    -- Fanstake Rivals: one lineup per user per weekly period is the entry equivalent.
    select
        cast(user_id as varchar)  as user_id,
        'fanstake'                as client_id,
        'pickem'                  as game_type,
        date_trunc('month', created_at::date) as entry_month
    from low6_azureeastus2.analytics_fanstake_rivals.fanstake_rivals__lineups
    where created_at::date >= '2025-07-01'
      and created_at::date <= current_date()

),

oilers_picks_entries as (

    -- Mart uses INNER JOIN on users and pickems; excludes entries for deleted
    -- users and DRAFT pickems.
    select
        cast(user_id as varchar)  as user_id,
        'oilers'                  as client_id,
        'pickem'                  as game_type,
        date_trunc('month', entered_at::date) as entry_month
    from low6_azureeastus2.analytics_oilers_picks.oilers_picks__entries
    where entered_at::date >= '2025-07-01'
      and entered_at::date <= current_date()

),

penn_entries as (

    -- Penn daily reveal. Canonical client_id ('penn') and game_type
    -- ('daily_reveal') carried in model.
    select
        cast(user_id as varchar)  as user_id,
        client_id,
        game_type,
        date_trunc('month', entered_at::date) as entry_month
    from low6_azureeastus2.analytics_penn.fct_penn__entries
    where entered_at::date >= '2025-07-01'
      and entered_at::date <= current_date()

),

pln_arcade_entries as (

    -- PLN Arcade mart exposes entered_date (DATE, ET) rather than a timestamp.
    select
        cast(user_id as varchar)  as user_id,
        'pln'                     as client_id,
        'pickem'                  as game_type,
        date_trunc('month', entered_date) as entry_month
    from low6_azureeastus2.analytics_pln_arcade.pln_arcade__entries
    where entered_date >= '2025-07-01'
      and entered_date <= current_date()

),

sackings_picks_entries as (

    -- Sackings: entry_date is MIN(selected_at_et) per user/contest cast to DATE.
    -- Mart filters is_active = 'TRUE'; inactive entries are excluded.
    select
        cast(user_id as varchar)  as user_id,
        'sackings'                as client_id,
        'pickem'                  as game_type,
        date_trunc('month', entry_date) as entry_month
    from low6_azureeastus2.analytics_sackings_picks.sackings_picks__entries
    where entry_date >= '2025-07-01'
      and entry_date <= current_date()

),

saracen_entries as (

    -- Saracen: sso_user_id is the canonical player identifier (numeric user_id
    -- is commented out in the source models). game_type is 'pickem' or 'bracket'.
    -- No client_id in model — hardcoded here.
    select
        cast(sso_user_id as varchar) as user_id,
        'saracen'                    as client_id,
        game_type,
        date_trunc('month', entered_at::date) as entry_month
    from low6_azureeastus2.analytics_saracen.fct_saracen__entries
    where entered_at::date >= '2025-07-01'
      and entered_at::date <= current_date()

),

all_entries as (

    select * from bet365_overunder_entries
    union all
    select * from bet365_uf_entries
    union all
    select * from bet99_picks_entries
    union all
    select * from betway_picks_entries
    union all
    select * from elf_entries
    union all
    select * from fanstake_entries
    union all
    select * from oilers_picks_entries
    union all
    select * from penn_entries
    union all
    select * from pln_arcade_entries
    union all
    select * from sackings_picks_entries
    union all
    select * from saracen_entries

),

monthly_users as (

    select
        client_id,
        game_type,
        entry_month,
        count(*)                as total_entries,
        count(distinct user_id) as mau
    from all_entries
    group by 1, 2, 3

),

summary as (

    select
        client_id,
        game_type,
        sum(total_entries) as total_entries,
        avg(mau)           as avg_mau
    from monthly_users
    group by 1, 2

),

-- active_users counts distinct users across the full period — cannot be derived
-- from summing MAU because the same user would be double-counted across months.
active_users as (

    select
        client_id,
        game_type,
        count(distinct user_id) as active_users
    from all_entries
    group by 1, 2

)

select
    s.client_id,
    s.game_type,
    s.total_entries,
    a.active_users,
    round(s.avg_mau, 0) as avg_mau
from summary as s
left join active_users as a
    on  s.client_id = a.client_id
    and s.game_type = a.game_type
order by
    s.client_id,
    s.game_type
;
