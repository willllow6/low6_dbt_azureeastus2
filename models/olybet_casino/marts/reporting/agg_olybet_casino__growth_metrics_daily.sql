with

date_spine as (

    select dateadd(day, seq4(), '{{ var("olybet_casino_start_date") }}'::date) as date_day
    from table(generator(rowcount => 700))
    where date_day <= current_date

),

dates as (

    select cast(date_day as date) as date_day
    from date_spine

),

registrations as (

    select
        registration_date_et    as date_day,
        count(*)                as registrations
    from {{ ref('dim_olybet_casino__users') }}
    group by 1

),

entries as (

    select
        entry_date_et,
        count(distinct case when user_entry_number = 1 then user_id end)    as first_entries,
        count(*)                                                            as total_entries
    from {{ ref('fct_olybet_casino__entries') }}
    group by 1

),

payouts as (

    select
        played_date_et               as date_day,
        count(*)                     as total_payouts,
        sum(prize_value)             as total_payout_value
    from {{ ref('fct_olybet_casino__tickets') }}
    where ticket_status = 'WIN'
    group by 1

),

free_bets as (

    select
        played_date_et               as date_day,
        count(*)                     as total_free_bets,
        sum(prize_value)             as total_free_bet_value
    from {{ ref('fct_olybet_casino__tickets') }}
    where ticket_status = 'WIN'
      and prize_type = 'FreeBet'
    group by 1

)

select
    d.date_day,
    'olybet'                                as client_id,
    'olybet'                                as tenant_id,
    'Olybet'                                as tenant_name,
    'instant_win'                           as game_type,
    coalesce(r.registrations,       0)      as registrations,
    coalesce(e.first_entries,       0)      as first_entries,
    coalesce(e.total_entries,       0)      as total_entries,
    coalesce(p.total_payouts,       0)      as total_payouts,
    coalesce(p.total_payout_value,  0)      as total_payout_value,
    coalesce(fb.total_free_bets,       0)   as total_free_bets,
    coalesce(fb.total_free_bet_value,  0)   as total_free_bet_value
from dates d
left join registrations r   on d.date_day = r.date_day
left join entries e         on d.date_day = e.entry_date_et
left join payouts p         on d.date_day = p.date_day
left join free_bets fb      on d.date_day = fb.date_day
