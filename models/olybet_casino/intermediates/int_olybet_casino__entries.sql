with

entries as (

    select *
    from {{ ref('stg_olybet_casino__competition_entries') }}

),

contests as (

    select
        contest_id,
        game_id,
        contest_name,
        contest_status,
        contest_start_date,
        contest_starts_at,
        contest_ends_at,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        is_active as contest_is_active,
        updated_at as contest_updated_at
    from {{ ref('stg_olybet_casino__competitions') }}

),

games as (

    select
        game_id,
        game_name
    from {{ ref('stg_olybet_casino__games') }}

),

tickets_agg as (

    select
        entry_id,
        count(*) as tickets_played,
        count(case when competition_prize_id is not null then 1 end) as tickets_won
    from {{ ref('stg_olybet_casino__competition_tickets') }}
    where entry_id is not null
    group by 1

),

joined as (

    select
        e.entry_id,
        e.user_id,
        e.contest_id,
        c.client_id,
        c.tenant_id,
        c.tenant_name,
        c.game_type,
        c.contest_name,
        c.contest_status,
        c.contest_start_date,
        c.contest_starts_at,
        c.contest_ends_at,
        g.game_name,
        coalesce(t.tickets_played, 0) as tickets_played,
        coalesce(t.tickets_won, 0)    as tickets_won,
        coalesce(t.tickets_won, 0) > 0 as is_winner,
        e.is_active,
        e.entry_date,
        e.entered_at,
        greatest(
            coalesce(e.updated_at,          '1900-01-01'::timestamp_ntz),
            coalesce(c.contest_updated_at,  '1900-01-01'::timestamp_ntz)
        ) as updated_at
    from entries e
    left join contests c
        on e.contest_id = c.contest_id
    left join games g
        on c.game_id = g.game_id
    left join tickets_agg t
        on e.entry_id = t.entry_id

),

with_entry_number as (

    select
        *,
        case
            when is_winner    then 'WINNER'
            when is_active    then 'ACTIVE'
            else                   'NO_WIN'
        end as entry_status,
        rank() over (
            partition by user_id
            order by entered_at
        ) as user_entry_number
    from joined

)

select * from with_entry_number
