with

user_cards as (

    select *
    from {{ ref('stg_penn__daily_reveal_user_cards') }}
    where is_selected = true

),

cards as (

    select
        card_id,
        display_name,
        first_name,
        last_name,
        position,
        team_name,
        team_abbreviation,
        jersey_number,
        sport
    from {{ ref('stg_penn__daily_reveal_cards') }}

),

entries as (

    select
        daily_reveal_entry_id,
        daily_reveal_day_id,
        user_id,
        client_id,
        tenant_id,
        game_type,
        total_hits,
        points
    from {{ ref('stg_penn__daily_reveal_entries') }}

),

days as (

    select
        daily_reveal_day_id,
        contest_id,
        day_number,
        game_date
    from {{ ref('stg_penn__daily_reveal_days') }}

),

users as (

    select
        user_id,
        sso_user_id,
        username
    from {{ ref('dim_penn__users') }}

),

joined as (

    select
        uc.user_card_id,
        uc.daily_reveal_entry_id,
        e.user_id || '-' || d.contest_id as entry_id,
        e.user_id,
        u.sso_user_id,
        u.username,
        d.contest_id,
        e.client_id,
        e.tenant_id,
        e.game_type,
        d.daily_reveal_day_id,
        d.day_number,
        d.game_date,
        c.card_id,
        c.display_name as player_name,
        c.first_name,
        c.last_name,
        c.position,
        c.team_name,
        c.team_abbreviation,
        c.sport,
        uc.rating as card_rating,
        uc.position as card_position,
        e.total_hits,
        e.points,
        uc.selected_at,
        convert_timezone('UTC', '{{ var("local_timezone") }}', uc.selected_at)::timestamp_ntz as selected_at_et,
        uc.created_at,
        row_number() over (
            partition by uc.daily_reveal_entry_id
            order by uc.selected_at desc
        ) as rn
    from user_cards uc
    inner join entries e
        on uc.daily_reveal_entry_id = e.daily_reveal_entry_id
    inner join days d
        on e.daily_reveal_day_id = d.daily_reveal_day_id
    inner join cards c
        on uc.card_id = c.card_id
    inner join users u
        on e.user_id = u.user_id

),

latest_selection as (

    select
        user_card_id,
        daily_reveal_entry_id,
        entry_id,
        user_id,
        sso_user_id,
        username,
        contest_id,
        client_id,
        tenant_id,
        game_type,
        daily_reveal_day_id,
        day_number,
        game_date,
        card_id,
        player_name,
        first_name,
        last_name,
        position,
        team_name,
        team_abbreviation,
        sport,
        card_rating,
        card_position,
        total_hits,
        points,
        selected_at,
        selected_at_et,
        created_at
    from joined
    where rn = 1

)

select * from latest_selection
