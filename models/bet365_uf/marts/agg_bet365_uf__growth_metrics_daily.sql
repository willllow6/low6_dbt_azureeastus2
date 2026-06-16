with

date_spine as (

    select dateadd(day, seq4(), '{{ var("bet365_uf_start_date") }}'::date) as date_day
    from table(generator(rowcount => 700))
    where date_day <= current_date

),

entries as (
    select user_id, client_id, game_type, entered_at, entry_number
    from {{ ref('fct_bet365_uf__entries') }}
),

users as (
    select user_id, client_id, registered_at, is_playable, game_type
    from {{ ref('stg_bet365_uf__users') }}
    where is_tester = false
),

purchases as (
    select user_id, purchased_at, purchase_price, user_purchase_number
    from {{ ref('fct_bet365_uf__app_store_purchases') }}
),

daus as (

    select
        active_date as date_day,
        count(*) as daily_active_users
    from {{ ref('mart_bet365_uf__user_active_days') }}
    group by 1

),

spine as (

    select
        date_spine.date_day,
        'bet365' as client_id,
        'fantasy' as game_type
    from date_spine

),

daily_entries as (

    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', entries.entered_at) as date) as date_day,
        entries.client_id,
        entries.game_type,
        count(distinct entries.user_id) as total_entrants,
        count(*) as total_entries,
        count(distinct case when entries.entry_number = 1 then entries.user_id end) as new_entrants,
        count(distinct case when entries.entry_number > 1 then entries.user_id end) as returning_entrants
    from entries
    inner join users
        on entries.user_id = users.user_id
    group by 1, 2, 3

),

daily_registrations as (

    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', users.registered_at) as date) as date_day,
        users.client_id,
        users.game_type,
        count(*) as new_registrations,
        count_if(is_playable) as playable_users
    from users
    group by 1, 2, 3

),

daily_revenue as (

    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', purchases.purchased_at) as date) as date_day,
        sum(purchases.purchase_price) as gross_revenue,
        count(*) as totaL_purchases,
        count_if(user_purchase_number = 1) as first_purchases,
        count(distinct purchases.user_id) as daily_purchasers
    from purchases
    inner join users
        on purchases.user_id = users.user_id
    group by 1

),

joined as (

    select
        spine.date_day,
        spine.client_id,
        spine.game_type,
        coalesce(dr.new_registrations, 0) as new_registrations,
        coalesce(dr.playable_users, 0) as playable_users,
        coalesce(de.total_entrants, 0) as total_entrants,
        coalesce(de.total_entries, 0) as total_entries,
        coalesce(de.new_entrants, 0) as new_entrants,
        coalesce(de.returning_entrants, 0) as returning_entrants,
        coalesce(drev.gross_revenue, 0) as gross_revenue,
        coalesce(drev.total_purchases, 0) as total_purchases,
        coalesce(drev.first_purchases, 0) as first_purchases,
        coalesce(drev.daily_purchasers, 0) as daily_purchasers,
        coalesce(daus.daily_active_users, 0) as daily_active_users
    from spine
    left join daily_entries as de
        on spine.date_day = de.date_day
        and spine.client_id = de.client_id
        and spine.game_type = de.game_type
    left join daily_registrations as dr
        on spine.date_day = dr.date_day
        and spine.client_id = dr.client_id
        and spine.game_type = dr.game_type
    left join daily_revenue as drev
        on spine.date_day = drev.date_day
    left join daus 
        on spine.date_day = daus.date_day

)

select
    date_day,
    client_id,
    game_type,
    new_registrations,
    playable_users,
    total_entrants,
    total_entries,
    new_entrants,
    returning_entrants,
    gross_revenue,
    total_purchases,
    first_purchases,
    daily_purchasers,
    daily_active_users

from joined
