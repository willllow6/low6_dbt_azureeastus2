with

entries as (

    select
        entry_id,
        user_id,
        contest_id,
        tournament_id,
        tenant_id,
        client_id,
        game_type,
        user_tier,
        status,
        locked_at,
        prize_outcome_id,
        prize_revealed_at,
        per_goal_amount,
        per_goal_currency,
        entry_date,
        entered_at,
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', entered_at) as date) as entry_date_et,
        day(cast(convert_timezone('UTC', '{{ var("local_timezone") }}', entered_at) as date)) as entry_day_et,
        convert_timezone('UTC', '{{ var("local_timezone") }}', entered_at)::timestamp_ntz as entered_at_et,
        updated_at
    from {{ ref('int_penn__entries') }}

),

with_entry_number as (

    select
        *,
        rank() over(
            partition by user_id
            order by entered_at
        ) as user_entry_number
    from entries

)

select * from with_entry_number
