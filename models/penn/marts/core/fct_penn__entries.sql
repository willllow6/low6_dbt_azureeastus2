with

daily_reveal_entries as (

    select
        entry_id,
        user_id,
        contest_id,
        client_id,
        tenant_id,
        game_type,
        points,
        days_entered,
        entry_date,
        entry_day,
        entered_at,
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', entered_at) as date) as entry_date_et,
        day(cast(convert_timezone('UTC', '{{ var("local_timezone") }}', entered_at) as date)) as entry_day_et,
        convert_timezone('UTC', '{{ var("local_timezone") }}', entered_at)::timestamp_ntz as entered_at_et,
        updated_at
    from {{ ref('int_penn__daily_reveal_entries') }}

),

with_entry_number as (

    select
        *,
        rank() over(
            partition by user_id
            order by entered_at
        ) as user_entry_number
    from daily_reveal_entries

)

select * from with_entry_number
