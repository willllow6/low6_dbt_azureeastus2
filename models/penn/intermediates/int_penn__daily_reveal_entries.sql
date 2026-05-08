with

daily_reveal_entries as (

    select *
    from {{ ref('stg_penn__daily_reveal_entries') }}

),

daily_reveal_days as (

    select
        daily_reveal_day_id,
        contest_id
    from {{ ref('stg_penn__daily_reveal_days') }}

),

joined as (

    select
        e.daily_reveal_entry_id,
        e.user_id,
        d.contest_id,
        e.client_id,
        e.tenant_id,
        e.game_type,
        e.points,
        e.created_at,
        e.created_date,
        e.updated_at
    from daily_reveal_entries e
    inner join daily_reveal_days d
        on e.daily_reveal_day_id = d.daily_reveal_day_id

),

aggregated as (

    select
        user_id || '-' || contest_id as entry_id,
        user_id,
        contest_id,
        client_id,
        tenant_id,
        game_type,
        sum(points) as points,
        count(*) as days_entered,
        min(created_date) as entry_date,
        day(min(created_date)) as entry_day,
        min(created_at) as entered_at,
        max(updated_at) as updated_at
    from joined
    group by 1, 2, 3, 4, 5, 6

)

select * from aggregated
