with

entries as (

    select *
    from {{ ref('fct_penn__entries') }}

),

users as (

    select *
    from {{ ref('dim_penn__users') }}

),

contests as (

    select *
    from {{ ref('dim_penn__contests') }}

),

joined as (

    select
        entries.entry_id,
        entries.user_id,
        users.sso_user_id,
        entries.contest_id,

        users.username,
        users.registration_date_et,

        contests.contest_title,
        contests.game_type,
        contests.contest_status,
        contests.contest_prize,
        contests.contest_opens_at,
        contests.contest_opens_at_et,
        contests.contest_start_date,
        contests.contest_starts_at,
        contests.contest_start_date_et,
        contests.contest_starts_at_et,

        entries.points,
        entries.days_entered,

        entries.user_entry_number,

        case
            when entries.user_entry_number = 1
                then 'First Entry'
            else 'Returning Entry'
        end as user_entry_type,

        entries.entry_date,
        entries.entry_day,
        hour(entries.entered_at) as entry_hour,
        entries.entered_at,
        entries.entry_date_et,
        entries.entry_day_et,
        hour(entries.entered_at_et) as entry_hour_et,
        entries.entered_at_et,
        greatest(
            coalesce(entries.updated_at, '1900-01-01'),
            coalesce(contests.updated_at, '1900-01-01')
        ) as updated_at

    from entries
        inner join users
            on entries.user_id = users.user_id
        inner join contests
            on entries.contest_id = contests.contest_id

)

select * from joined
