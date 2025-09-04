with

picks as (

    select *
    from {{ ref('bet365_overunder__picks') }}

),

pick_stats as (

    select
        picked_option,
        player_name,
        player_position,
        market_name,
        market_handicap,
        country,
        state_province,
        segment_group,
        entry_date,
        count(*) as picks,
        sum(case when is_correct then 1 else 0 end) as correct_picks
    from picks
    group by 1,2,3,4,5,6,7,8,9

)

select * from pick_stats