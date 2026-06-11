with

date_spine as (

    select
        dateadd(
            day,
            seq4(),
            cast('{{ var("gana_gamezone_start_date") }}' as date)
        )                                           as date_day
    from table(generator(rowcount => 730))
    where date_day <= current_date

),

registrations as (

    select
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', registered_at) as date)   as date_day,
        count(distinct user_id)                     as new_registrations
    from {{ ref('fct_gana_gamezone__registrations') }}
    group by 1

),

predictor_entries as (

    select
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', entered_at) as date)   as date_day,
        count(entry_id)                             as predictor_entries,
        count(distinct user_id)                     as predictor_unique_entrants
    from {{ ref('fct_gana_gamezone__predictor_entries') }}
    group by 1

),

survivor_picks as (

    select
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', selected_at) as date)   as date_day,
        count(entry_id)                             as survivor_picks,
        count(distinct user_id)                     as survivor_unique_pickers
    from {{ ref('fct_gana_gamezone__survivor_entries') }}
    group by 1

),

joined as (

    select
        d.date_day,
        'gana'                                      as client_id,
        coalesce(r.new_registrations, 0)            as new_registrations,
        coalesce(pe.predictor_entries, 0)           as predictor_entries,
        coalesce(pe.predictor_unique_entrants, 0)   as predictor_unique_entrants,
        coalesce(sp.survivor_picks, 0)              as survivor_picks,
        coalesce(sp.survivor_unique_pickers, 0)     as survivor_unique_pickers
    from date_spine as d
    left join registrations as r
        on d.date_day = r.date_day
    left join predictor_entries as pe
        on d.date_day = pe.date_day
    left join survivor_picks as sp
        on d.date_day = sp.date_day

)

select * from joined
