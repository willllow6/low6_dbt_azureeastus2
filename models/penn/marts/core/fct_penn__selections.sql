with

user_cards as (

    select *
    from {{ ref('stg_penn__daily_reveal_user_cards') }}

),

entries as (

    select
        daily_reveal_entry_id,
        daily_reveal_day_id,
        user_id,
        client_id,
        tenant_id,
        game_type
    from {{ ref('stg_penn__daily_reveal_entries') }}

),

days as (

    select
        daily_reveal_day_id,
        contest_id
    from {{ ref('stg_penn__daily_reveal_days') }}

),

joined as (

    select
        uc.user_card_id as selection_id,
        uc.daily_reveal_entry_id as entry_id,
        e.user_id,
        d.contest_id,
        e.client_id,
        e.tenant_id,
        e.game_type,
        uc.card_id,
        uc.rating,
        uc.position,
        uc.is_selected,
        uc.selected_at,
        convert_timezone('UTC', '{{ var("local_timezone") }}', uc.selected_at)::timestamp_ntz as selected_at_et,
        uc.created_at,
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', uc.created_at) as date) as created_date_et
    from user_cards uc
    inner join entries e
        on uc.daily_reveal_entry_id = e.daily_reveal_entry_id
    inner join days d
        on e.daily_reveal_day_id = d.daily_reveal_day_id

)

select * from joined
