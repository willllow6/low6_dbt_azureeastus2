with

users as (

    select *
    from {{ ref('dim_bet365_overunder__users') }}

),

entries as (

    select *
    from {{ ref('fct_bet365_overunder__entries') }}

),

user_activity as (

    select *
    from {{ ref('fct_bet365_overunder__user_activity_daily') }}

),

weekly_registrations as (

    select
        dateadd('day', 1, date_trunc('week', dateadd('day', -1, registration_date_et))) as week_start_et,
        client_id,
        tenant_id,
        game_type,
        country,
        count(*) as registrations
    from users
    group by 1, 2, 3, 4, 5

),

weekly_active_users as (

    select
        dateadd('day', 1, date_trunc('week', dateadd('day', -1, activity_date_et))) as week_start_et,
        client_id,
        tenant_id,
        game_type,
        country,
        count(distinct user_id) as active_users
    from user_activity
    group by 1, 2, 3, 4, 5

),

weekly_entries as (

    select
        dateadd('day', 1, date_trunc('week', dateadd('day', -1, entry_date_et))) as week_start_et,
        client_id,
        tenant_id,
        game_type,
        country,
        count(*) as total_entries,
        count_if(user_entry_number = 1) as new_players,
        count_if(user_season_entry_number = 1) as new_players_this_season,
        count_if(is_winner) as winning_entries,
        sum(case when currency_code = 'CAD' then prize_amount else 0 end) as payouts_cad,
        sum(case when currency_code = 'USD' then prize_amount else 0 end) as payouts_usd
    from entries
    group by 1, 2, 3, 4, 5

),

unioned as (

    select
        week_start_et, client_id, tenant_id, game_type, country,
        registrations,
        0 as active_users,
        0 as total_entries,
        0 as new_players,
        0 as new_players_this_season,
        0 as winning_entries,
        0 as payouts_cad,
        0 as payouts_usd
    from weekly_registrations

    union all

    select
        week_start_et, client_id, tenant_id, game_type, country,
        0 as registrations,
        active_users,
        0 as total_entries,
        0 as new_players,
        0 as new_players_this_season,
        0 as winning_entries,
        0 as payouts_cad,
        0 as payouts_usd
    from weekly_active_users

    union all

    select
        week_start_et, client_id, tenant_id, game_type, country,
        0 as registrations,
        0 as active_users,
        total_entries,
        new_players,
        new_players_this_season,
        winning_entries,
        payouts_cad,
        payouts_usd
    from weekly_entries

),

aggregated as (

    select
        week_start_et,
        client_id,
        tenant_id,
        game_type,
        country,
        sum(registrations) as registrations,
        sum(active_users) as active_users,
        sum(total_entries) as total_entries,
        sum(new_players) as new_players,
        sum(new_players_this_season) as new_players_this_season,
        sum(winning_entries) as winning_entries,
        sum(payouts_cad) as payouts_cad,
        sum(payouts_usd) as payouts_usd
    from unioned
    group by 1, 2, 3, 4, 5

)

select
    week_start_et,
    client_id,
    tenant_id,
    game_type,
    country,

    registrations,
    active_users,
    total_entries,
    new_players,
    new_players_this_season,
    winning_entries,
    payouts_cad,
    payouts_usd

from aggregated
