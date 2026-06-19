with

entries as (

    select *
    from {{ ref('mart_saracen__entries') }}

),

bracket_contests as (

    select contest_sk, tournament_name
    from {{ ref('int_saracen_bracket__contests_unioned') }}

),

joined as (

    select
        entries.contest_sk,
        case
            when entries.game_type = 'bracket'
                then bracket_contests.tournament_name
            else entries.contest_name
        end as contest_name,
        entries.contest_status,
        entries.game_type,
        entries.contest_starts_at_et,
        entries.user_game_entry_number
    from entries
    left join bracket_contests
        on entries.contest_sk = bracket_contests.contest_sk

),

entry_stats as (

    select
        contest_sk,
        contest_name,
        contest_status,
        game_type,
        contest_starts_at_et,
        count(*) as total_entries,
        sum(case when user_game_entry_number = 1 then 1 else 0 end) as first_game_entries,
        sum(case when user_game_entry_number > 1 then 1 else 0 end) as returning_game_entries
    from joined
    group by 1, 2, 3, 4, 5

)

select * from entry_stats
