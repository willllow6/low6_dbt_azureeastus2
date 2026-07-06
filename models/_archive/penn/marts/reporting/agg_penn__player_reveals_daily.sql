with

reveals as (

    select
        selection_id,
        user_id,
        contest_id,
        tenant_id,
        client_id,
        game_type,
        revealed_player_id,
        cast(revealed_at_et as date) as reveal_date
    from {{ ref('fct_penn__selections') }}
    where status = 'revealed'
        or revealed_at is not null

),

players as (

    select
        player_id,
        first_name,
        last_name,
        position,
        number
    from {{ ref('stg_penn__players') }}

),

tenants as (

    select
        tenant_id,
        tenant_name
    from {{ ref('dim_penn__tenants') }}

),

daily as (

    select
        r.reveal_date as date_day,
        r.client_id,
        r.tenant_id,
        t.tenant_name,
        r.game_type,
        p.player_id,
        p.first_name,
        p.last_name,
        p.position,
        p.number,
        count(r.selection_id) as total_reveals,
        count(distinct r.user_id) as unique_users,
        count(distinct r.contest_id) as unique_contests
    from reveals as r
    left join players as p
        on r.revealed_player_id = p.player_id
    left join tenants as t
        on r.tenant_id = t.tenant_id
    group by
        r.reveal_date,
        r.client_id,
        r.tenant_id,
        t.tenant_name,
        r.game_type,
        p.player_id,
        p.first_name,
        p.last_name,
        p.position,
        p.number

)

select * from daily
