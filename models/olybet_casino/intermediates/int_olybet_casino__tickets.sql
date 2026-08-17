with

tickets as (

    select *
    from {{ ref('stg_olybet_casino__competition_tickets') }}

),

entries as (

    select
        entry_id,
        user_id
    from {{ ref('stg_olybet_casino__competition_entries') }}

),

contests as (

    select
        contest_id,
        contest_name,
        client_id,
        tenant_id,
        tenant_name,
        game_type
    from {{ ref('stg_olybet_casino__competitions') }}

),

competition_prizes as (

    select
        competition_prize_id,
        prize_id,
        prize_tier,
        win_chance
    from {{ ref('stg_olybet_casino__competition_prizes') }}

),

prizes as (

    select
        prize_id,
        prize_name,
        prize_type,
        prize_value
    from {{ ref('stg_olybet_casino__prizes') }}

),

joined as (

    select
        t.ticket_id,
        t.entry_id,
        e.user_id,
        t.contest_id,
        c.contest_name,
        c.client_id,
        c.tenant_id,
        c.tenant_name,
        c.game_type,
        t.competition_prize_id,
        cp.prize_tier,
        cp.win_chance,
        cp.prize_id,
        p.prize_name,
        p.prize_type,
        p.prize_value,
        case
            when t.competition_prize_id is not null                          then 'WIN'
            when t.played_at is not null and t.competition_prize_id is null  then 'LOSE'
            else                                                                  'PENDING'
        end as ticket_status,
        t.is_played,
        t.played_date,
        t.played_at,
        t.created_at,
        t.updated_at
    from tickets t
    left join entries e
        on t.entry_id = e.entry_id
    left join contests c
        on t.contest_id = c.contest_id
    left join competition_prizes cp
        on t.competition_prize_id = cp.competition_prize_id
    left join prizes p
        on cp.prize_id = p.prize_id

)

select * from joined
