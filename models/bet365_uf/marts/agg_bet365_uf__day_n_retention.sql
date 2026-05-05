with

entries as (

    select
        * exclude (entry_date),
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', entered_at) as date) as entry_date
    from {{ ref('mart_bet365_uf__entries') }}
    where is_tester = false

),

d1_retention as (

    select
        count(distinct entries.user_id) as d1_new_users,
        count(distinct future_entries.user_id) as d1_retained_users,
        div0(count(distinct future_entries.user_id), count(distinct entries.user_id))::float as d1_retention
    from entries
    left join entries as future_entries
        on entries.user_id = future_entries.user_id
        and entries.entry_date = future_entries.entry_date - interval '1 day'
    where entries.entry_number = 1
    and entries.entry_date < current_date - interval '1 day'

),

d7_retention as (

    select
        count(distinct entries.user_id) as d7_new_users,
        count(distinct future_entries.user_id) as d7_retained_users,
        div0(count(distinct future_entries.user_id), count(distinct entries.user_id))::float as d7_retention
    from entries
    left join entries as future_entries
        on entries.user_id = future_entries.user_id
        and entries.entry_date = future_entries.entry_date - interval '7 day'
    where entries.entry_number = 1
    and entries.entry_date < current_date - interval '7 day'

),

d14_retention as (

    select
        count(distinct entries.user_id) as d14_new_users,
        count(distinct future_entries.user_id) as d14_retained_users,
        div0(count(distinct future_entries.user_id), count(distinct entries.user_id))::float as d14_retention
    from entries
    left join entries as future_entries
        on entries.user_id = future_entries.user_id
        and entries.entry_date = future_entries.entry_date - interval '14 day'
    where entries.entry_number = 1
    and entries.entry_date < current_date - interval '14 day'

),

d28_retention as (

    select
        count(distinct entries.user_id) as d28_new_users,
        count(distinct future_entries.user_id) as d28_retained_users,
        div0(count(distinct future_entries.user_id), count(distinct entries.user_id))::float as d28_retention
    from entries
    left join entries as future_entries
        on entries.user_id = future_entries.user_id
        and entries.entry_date = future_entries.entry_date - interval '28 day'
    where entries.entry_number = 1
    and entries.entry_date < current_date - interval '28 day'

),

maus as (

    select
        count(distinct user_id) as maus
    from entries
    where
        entry_date < current_date
        and entry_date >= current_date - 29

)

select
    d1.d1_retention,
    d7.d7_retention,
    d14.d14_retention,
    d28.d28_retention,
    mau.maus
from d1_retention as d1
    full join d7_retention as d7
    full join d14_retention as d14
    full join d28_retention as d28
    full join maus as mau
