with

date_spine as (

    select dateadd(day, seq4(), '{{ var("opap_spintowin_start_date") }}'::date) as date_day
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
    from {{ ref('dim_opap_spintowin__users') }}
    group by 1

),

entries as (

    select
        entry_date_et,
        count(distinct case when user_entry_number = 1 then user_id end)    as first_entries,
        count(*)                                                            as total_entries
    from {{ ref('fct_opap_spintowin__entries') }}
    group by 1

),

payouts as (

    select
        created_date_et             as date_day,
        count(*)                    as total_payouts,
        sum(prize_amount)           as total_payout_value
    from {{ ref('fct_opap_spintowin__payouts') }}
    group by 1

)

select
    d.date_day,
    'opap'                              as client_id,
    'opap'                              as tenant_id,
    'OPAP'                              as tenant_name,
    'spin_to_win'                       as game_type,
    coalesce(r.registrations,       0)  as registrations,
    coalesce(e.first_entries,       0)  as first_entries,
    coalesce(e.total_entries,       0)  as total_entries,
    coalesce(p.total_payouts,       0)  as total_payouts,
    coalesce(p.total_payout_value,  0)  as total_payout_value
from dates d
left join registrations r   on d.date_day = r.date_day
left join entries e         on d.date_day = e.entry_date_et
left join payouts p         on d.date_day = p.date_day
