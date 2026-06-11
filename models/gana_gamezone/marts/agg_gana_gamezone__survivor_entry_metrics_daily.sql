with

entries as (

    select * from {{ ref('fct_gana_gamezone__survivor_entries') }}

),

daily as (

    select
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', selected_at) as date)   as date_day,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        count(entry_id)                                                                 as total_picks,
        count(distinct user_id)                                                         as active_users,
        sum(case when is_correct then 1 else 0 end)                                     as correct_picks,
        sum(case when is_correct is not null then 1 else 0 end)                         as scored_picks,
        div0(
            sum(case when is_correct then 1 else 0 end),
            sum(case when is_correct is not null then 1 else 0 end)
        )                                                                               as accuracy_rate
    from entries
    where selected_country_id is not null
    group by 1, 2, 3, 4, 5

)

select * from daily
