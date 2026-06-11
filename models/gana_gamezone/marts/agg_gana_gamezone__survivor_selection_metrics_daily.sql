with

entries as (

    select * from {{ ref('mart_gana_gamezone__survivor_entries') }}

),

aggregated as (

    select
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', selected_at) as date)   as date_day,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        contest_id,
        contest_name,
        competing_teams,
        day,
        selected_country_id,
        selected_country_name,
        count(*)                                                                         as total_picks,
        sum(case when is_correct then 1 else 0 end)                                     as correct_picks,
        div0(
            sum(case when is_correct then 1 else 0 end),
            sum(case when is_correct is not null then 1 else 0 end)
        )                                                                                as accuracy_rate
    from entries
    where selected_country_id is not null
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11

)

select * from aggregated
