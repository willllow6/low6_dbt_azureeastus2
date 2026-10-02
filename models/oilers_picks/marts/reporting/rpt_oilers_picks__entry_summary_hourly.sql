with

entries as (

    select *
    from {{ ref('fct_oilers_picks__entries') }}

),

users as (

    select *
    from {{ ref('dim_oilers_picks__users') }}

),

contests as (

    select *
    from {{ ref('dim_oilers_picks__contests') }}

),

joined as (

    select
        entries.contest_id,
        entries.client_id,
        entries.tenant_id,
        entries.tenant_name,
        entries.game_type,

        contests.contest_name,
        contests.contest_status,
        contests.contest_start_date_et,

        entries.entry_date_et,
        hour(entries.entered_at_et) as entry_hour_et,
        case
            when entries.user_entry_number = 1
                then 'First Entry'
            else 'Subsequent Entry'
        end as entry_type,
        users.marketing_consent,
        users.has_placed_sports_bet,

        entries.user_entry_number

    from entries
    inner join users
        on entries.user_id = users.user_id
    inner join contests
        on entries.contest_id = contests.contest_id

)

select
    contest_id,
    entry_date_et,
    entry_hour_et,
    entry_type,
    marketing_consent,
    has_placed_sports_bet,

    client_id,
    tenant_id,
    tenant_name,
    game_type,

    contest_name,
    contest_status,
    contest_start_date_et,

    count(*) as entries,
    count_if(user_entry_number = 1) as first_entries

from joined
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
