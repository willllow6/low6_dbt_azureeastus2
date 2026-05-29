with

selections as (

    select *
    from {{ ref('fct_penn__selections') }}

),

players as (

    select
        player_id,
        external_id,
        first_name,
        last_name,
        position,
        number,
        birth_country
    from {{ ref('stg_penn__players') }}

),

users as (

    select
        user_id,
        penn_user_id
    from {{ ref('dim_penn__users') }}

),

contests as (

    select
        contest_id,
        contest_name,
        tournament_name,
        tenant_name,
        game_type
    from {{ ref('dim_penn__contests') }}

),

joined as (

    select
        s.selection_id,
        s.entry_id,
        s.user_id,
        u.penn_user_id,
        s.contest_id,
        s.tournament_id,
        s.tenant_id,
        s.client_id,
        s.game_type,
        c.contest_name,
        c.tournament_name,
        c.tenant_name,
        s.day_index,
        s.status,
        p.player_id,
        p.first_name,
        p.last_name,
        p.position,
        p.number,
        p.birth_country,
        s.revealed_rarity,
        s.revealed_at,
        s.revealed_at_et,
        s.reveal_start,
        s.reveal_end,
        s.created_at,
        s.updated_at
    from selections s
    left join players p
        on s.revealed_player_id = p.player_id
    inner join users u
        on s.user_id = u.user_id
    inner join contests c
        on s.contest_id = c.contest_id

)

select * from joined
