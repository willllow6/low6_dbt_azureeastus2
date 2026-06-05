with

contests as (

    select *
    from {{ ref('dim_opap_spintowin__contests') }}

),

entries as (

    select
        contest_id,
        count(*)                                                            as total_entries,
        count(distinct user_id)                                             as unique_entrants,
        count(distinct case when user_entry_number = 1 then user_id end)    as first_time_entrants,
        count(case when entry_status = 'WINNER' then 1 end)                 as winners
    from {{ ref('fct_opap_spintowin__entries') }}
    group by 1

),

payouts as (

    select
        contest_id,
        count(*)            as total_payouts,
        sum(prize_amount)   as total_payout_value
    from {{ ref('fct_opap_spintowin__payouts') }}
    group by 1

)

select
    c.contest_id,
    c.client_id,
    c.tenant_id,
    c.tenant_name,
    c.game_type,
    c.contest_title,
    c.contest_status,
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
    coalesce(p.total_payouts,       0)      as total_payouts,
    coalesce(p.total_payout_value,  0)      as total_payout_value
from contests c
left join entries e  on c.contest_id = e.contest_id
left join payouts p  on c.contest_id = p.contest_id
