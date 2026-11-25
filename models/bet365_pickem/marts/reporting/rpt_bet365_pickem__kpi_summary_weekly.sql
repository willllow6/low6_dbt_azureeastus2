with

users as (

    select *
    from {{ ref('dim_bet365_pickem__users') }}

),

entries as (

    select *
    from {{ ref('fct_bet365_pickem__entries') }}

),

payouts as (

    select *
    from {{ ref('fct_bet365_pickem__payouts') }}

),

user_activity as (

    select *
    from {{ ref('fct_bet365_pickem__user_activity_daily') }}

),

tenants as (

    select *
    from {{ ref('dim_bet365_pickem__tenants') }}

),

-- one row per paid entry, so the join to entries can't fan out
entry_payouts as (

    select
        entry_id,
        count_if(currency = 'CAD' and place_amount > 0) as leaderboard_payout_count_cad,
        count_if(currency = 'CAD' and jackpot_amount > 0) as jackpot_payout_count_cad,
        count_if(currency = 'USD' and place_amount > 0) as leaderboard_payout_count_usd,
        count_if(currency = 'USD' and jackpot_amount > 0) as jackpot_payout_count_usd,
        sum(case when currency = 'CAD' then coalesce(place_amount, 0) else 0 end) as leaderboard_payouts_cad,
        sum(case when currency = 'CAD' then coalesce(jackpot_amount, 0) else 0 end) as jackpot_payouts_cad,
        sum(case when currency = 'CAD' then prize_amount else 0 end) as payouts_cad,
        sum(case when currency = 'USD' then coalesce(place_amount, 0) else 0 end) as leaderboard_payouts_usd,
        sum(case when currency = 'USD' then coalesce(jackpot_amount, 0) else 0 end) as jackpot_payouts_usd,
        sum(case when currency = 'USD' then prize_amount else 0 end) as payouts_usd
    from payouts
    group by 1

),

weekly_registrations as (

    select
        date_trunc('week', registration_date_et) as week_start_et,
        client_id,
        tenant_id,
        game_type,
        count(*) as registrations
    from users
    group by 1, 2, 3, 4

),

weekly_active_users as (

    select
        date_trunc('week', activity_date_et) as week_start_et,
        client_id,
        tenant_id,
        game_type,
        count(distinct user_id) as active_users
    from user_activity
    group by 1, 2, 3, 4

),

weekly_entries as (

    select
        date_trunc('week', entries.entry_date_et) as week_start_et,
        entries.client_id,
        entries.tenant_id,
        entries.game_type,
        count(*) as total_entries,
        count_if(entries.user_entry_number = 1) as new_players,
        count(entry_payouts.entry_id) as winning_entries,
        coalesce(sum(entry_payouts.leaderboard_payout_count_cad), 0) as leaderboard_payout_count_cad,
        coalesce(sum(entry_payouts.jackpot_payout_count_cad), 0) as jackpot_payout_count_cad,
        coalesce(sum(entry_payouts.leaderboard_payout_count_usd), 0) as leaderboard_payout_count_usd,
        coalesce(sum(entry_payouts.jackpot_payout_count_usd), 0) as jackpot_payout_count_usd,
        coalesce(sum(entry_payouts.leaderboard_payouts_cad), 0) as leaderboard_payouts_cad,
        coalesce(sum(entry_payouts.jackpot_payouts_cad), 0) as jackpot_payouts_cad,
        coalesce(sum(entry_payouts.payouts_cad), 0) as payouts_cad,
        coalesce(sum(entry_payouts.leaderboard_payouts_usd), 0) as leaderboard_payouts_usd,
        coalesce(sum(entry_payouts.jackpot_payouts_usd), 0) as jackpot_payouts_usd,
        coalesce(sum(entry_payouts.payouts_usd), 0) as payouts_usd
    from entries
    left join entry_payouts
        on entries.entry_id = entry_payouts.entry_id
    group by 1, 2, 3, 4

),

unioned as (

    select
        week_start_et, client_id, tenant_id, game_type,
        registrations,
        0 as active_users,
        0 as total_entries,
        0 as new_players,
        0 as winning_entries,
        0 as leaderboard_payout_count_cad,
        0 as jackpot_payout_count_cad,
        0 as leaderboard_payout_count_usd,
        0 as jackpot_payout_count_usd,
        0 as leaderboard_payouts_cad,
        0 as jackpot_payouts_cad,
        0 as payouts_cad,
        0 as leaderboard_payouts_usd,
        0 as jackpot_payouts_usd,
        0 as payouts_usd
    from weekly_registrations

    union all

    select
        week_start_et, client_id, tenant_id, game_type,
        0 as registrations,
        active_users,
        0 as total_entries,
        0 as new_players,
        0 as winning_entries,
        0 as leaderboard_payout_count_cad,
        0 as jackpot_payout_count_cad,
        0 as leaderboard_payout_count_usd,
        0 as jackpot_payout_count_usd,
        0 as leaderboard_payouts_cad,
        0 as jackpot_payouts_cad,
        0 as payouts_cad,
        0 as leaderboard_payouts_usd,
        0 as jackpot_payouts_usd,
        0 as payouts_usd
    from weekly_active_users

    union all

    select
        week_start_et, client_id, tenant_id, game_type,
        0 as registrations,
        0 as active_users,
        total_entries,
        new_players,
        winning_entries,
        leaderboard_payout_count_cad,
        jackpot_payout_count_cad,
        leaderboard_payout_count_usd,
        jackpot_payout_count_usd,
        leaderboard_payouts_cad,
        jackpot_payouts_cad,
        payouts_cad,
        leaderboard_payouts_usd,
        jackpot_payouts_usd,
        payouts_usd
    from weekly_entries

),

aggregated as (

    select
        week_start_et,
        client_id,
        tenant_id,
        game_type,
        sum(registrations) as registrations,
        sum(active_users) as active_users,
        sum(total_entries) as total_entries,
        sum(new_players) as new_players,
        sum(winning_entries) as winning_entries,
        sum(leaderboard_payout_count_cad) as leaderboard_payout_count_cad,
        sum(jackpot_payout_count_cad) as jackpot_payout_count_cad,
        sum(leaderboard_payout_count_usd) as leaderboard_payout_count_usd,
        sum(jackpot_payout_count_usd) as jackpot_payout_count_usd,
        sum(leaderboard_payouts_cad) as leaderboard_payouts_cad,
        sum(jackpot_payouts_cad) as jackpot_payouts_cad,
        sum(payouts_cad) as payouts_cad,
        sum(leaderboard_payouts_usd) as leaderboard_payouts_usd,
        sum(jackpot_payouts_usd) as jackpot_payouts_usd,
        sum(payouts_usd) as payouts_usd
    from unioned
    group by 1, 2, 3, 4

)

select
    aggregated.week_start_et,
    aggregated.client_id,
    aggregated.tenant_id,
    tenants.tenant_name,
    tenants.country_code,
    aggregated.game_type,

    aggregated.registrations,
    aggregated.active_users,
    aggregated.total_entries,
    aggregated.new_players,
    aggregated.winning_entries,
    aggregated.leaderboard_payout_count_cad,
    aggregated.jackpot_payout_count_cad,
    aggregated.leaderboard_payout_count_usd,
    aggregated.jackpot_payout_count_usd,
    aggregated.leaderboard_payouts_cad,
    aggregated.jackpot_payouts_cad,
    aggregated.payouts_cad,
    aggregated.leaderboard_payouts_usd,
    aggregated.jackpot_payouts_usd,
    aggregated.payouts_usd

from aggregated
left join tenants
    on aggregated.tenant_id = tenants.tenant_id
