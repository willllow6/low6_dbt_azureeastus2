with

selections as (

    select *
    from {{ ref('fct_opap_spintowin__selections') }}

),

contests as (

    select
        contest_id,
        contest_title,
        contest_status,
        contest_start_date,
        contest_starts_at
    from {{ ref('dim_opap_spintowin__contests') }}

),

contest_entries as (

    select
        contest_id,
        count(*) as total_entries
    from {{ ref('fct_opap_spintowin__entries') }}
    group by 1

),

aggregated as (

    select
        contest_id,
        client_id,
        tenant_id,
        game_type,
        event_id,
        event_name,
        market_id,
        market_name,
        selection_sequence,
        selection_value,
        count(*)                                                    as total_selections,
        count(case when selection_status = 'WIN' then 1 end)        as correct_selections
    from selections
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

)

select
    a.contest_id,
    c.contest_title,
    c.contest_status,
    c.contest_start_date,
    c.contest_starts_at,
    a.client_id,
    a.tenant_id,
    a.game_type,
    a.event_id,
    a.event_name,
    a.market_id,
    a.market_name,
    a.selection_sequence,
    a.selection_value,
    a.total_selections,
    a.correct_selections,
    ce.total_entries,
    case
        when ce.total_entries > 0
            then round(a.total_selections / ce.total_entries::float, 4)
        else null
    end                                                             as selection_rate
from aggregated a
left join contests c
    on a.contest_id = c.contest_id
left join contest_entries ce
    on a.contest_id = ce.contest_id
