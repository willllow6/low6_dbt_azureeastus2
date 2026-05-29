with

user_slots as (

    select *
    from {{ ref('stg_penn__user_slots') }}

),

user_rounds as (

    select
        user_round_id,
        user_id,
        round_id
    from {{ ref('stg_penn__user_rounds') }}

),

rounds as (

    select
        round_id,
        tournament_id
    from {{ ref('stg_penn__rounds') }}

),

tournaments as (

    select
        tournament_id,
        tenant_id
    from {{ ref('stg_penn__tournaments') }}

),

joined as (

    select
        s.user_slot_id as selection_id,
        s.user_round_id as entry_id,
        ur.user_id,
        r.round_id as contest_id,
        r.tournament_id,
        t.tenant_id,
        'penn' as client_id,
        'squads' as game_type,
        s.day_index,
        s.status,
        s.revealed_player_id,
        s.revealed_rarity,
        s.reveal_start,
        s.reveal_end,
        s.revealed_at,
        convert_timezone('UTC', '{{ var("local_timezone") }}', s.revealed_at)::timestamp_ntz as revealed_at_et,
        s.created_at,
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', s.created_at) as date) as created_date_et,
        s.updated_at
    from user_slots s
    inner join user_rounds ur
        on s.user_round_id = ur.user_round_id
    inner join rounds r
        on ur.round_id = r.round_id
    inner join tournaments t
        on r.tournament_id = t.tournament_id

)

select * from joined
