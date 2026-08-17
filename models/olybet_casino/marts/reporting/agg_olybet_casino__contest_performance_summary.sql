with

contests as (

    select *
    from {{ ref('dim_olybet_casino__contests') }}

),

entries as (

    select
        contest_id,
        count(*)                                                            as total_entries,
        count(distinct user_id)                                             as unique_entrants,
        count(distinct case when user_entry_number = 1 then user_id end)    as first_time_entrants,
        count(case when entry_status = 'WINNER' then 1 end)                 as winners
    from {{ ref('fct_olybet_casino__entries') }}
    group by 1

),

tickets as (

    select
        contest_id,
        count(*)                                              as tickets_played,
        count(case when ticket_status = 'WIN' then 1 end)     as tickets_won,
        sum(case when ticket_status = 'WIN' then prize_value end) as total_prize_value
    from {{ ref('fct_olybet_casino__tickets') }}
    group by 1

),

free_bets as (

    select
        contest_id,
        count(*)          as total_free_bets,
        sum(prize_value)  as total_free_bet_value
    from {{ ref('fct_olybet_casino__tickets') }}
    where ticket_status = 'WIN'
      and prize_type = 'FreeBet'
    group by 1

)

select
    c.contest_id,
    c.client_id,
    c.tenant_id,
    c.tenant_name,
    c.game_type,
    c.contest_name,
    c.contest_status,
    c.game_name,
    c.contest_start_date,
    c.contest_starts_at,
    coalesce(e.total_entries,       0)      as total_entries,
    coalesce(e.unique_entrants,     0)      as unique_entrants,
    coalesce(e.first_time_entrants, 0)      as first_time_entrants,
    coalesce(e.winners,             0)      as winners,
    case
        when e.total_entries > 0
            then round(e.winners / e.total_entries::float, 4)
        else null
    end                                     as win_rate,
    coalesce(t.tickets_played,      0)      as tickets_played,
    coalesce(t.tickets_won,         0)      as tickets_won,
    coalesce(t.total_prize_value,   0)      as total_prize_value,
    coalesce(fb.total_free_bets,      0)    as total_free_bets,
    coalesce(fb.total_free_bet_value, 0)    as total_free_bet_value
from contests c
left join entries e     on c.contest_id = e.contest_id
left join tickets t     on c.contest_id = t.contest_id
left join free_bets fb  on c.contest_id = fb.contest_id
