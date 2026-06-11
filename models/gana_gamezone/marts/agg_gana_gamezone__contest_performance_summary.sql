with

-- Predictor: one row for the whole game
predictor as (

    select
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        contest_id,
        'World Cup Predictor'                                                                               as contest_name,
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', min(entered_at)) as date) as contest_date,
        count(distinct user_id)                                                                             as total_entrants,
        null::integer                                                                                       as correct_entrants,
        null::float                                                                                         as accuracy_rate
    from {{ ref('mart_gana_gamezone__predictor_entries') }}
    group by 1, 2, 3, 4, 5

),

-- Survivor: one row per match day
survivor as (

    select
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        contest_id,
        contest_name,
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', min(ends_at)) as date)   as contest_date,
        count(distinct user_id)                                                                             as total_entrants,
        sum(case when is_correct then 1 else 0 end)::integer                                               as correct_entrants,
        div0(
            sum(case when is_correct then 1 else 0 end),
            sum(case when is_correct is not null then 1 else 0 end)
        )                                                                                                   as accuracy_rate
    from {{ ref('mart_gana_gamezone__survivor_entries') }}
    where selected_country_id is not null
    group by 1, 2, 3, 4, 5, 6

)

select * from predictor
union all
select * from survivor
