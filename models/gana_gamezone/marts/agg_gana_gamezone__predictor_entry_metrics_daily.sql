with

entries as (

    select * from {{ ref('fct_gana_gamezone__predictor_entries') }}

),

daily as (

    select
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', entered_at) as date)   as date_day,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        count(entry_id)                                             as total_entries,
        count(distinct user_id)                                     as unique_entrants,
        sum(case when entry_number = 1 then 1 else 0 end)          as first_time_entrants
    from entries
    group by 1, 2, 3, 4, 5

)

select * from daily
