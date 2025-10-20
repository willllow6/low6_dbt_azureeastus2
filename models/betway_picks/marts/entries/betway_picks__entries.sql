with

user_selections as (

    select * 
    from {{ ref('stg_betway_picks__user_selections') }}

),

users as (

    select * 
    from {{ ref('stg_betway_picks__users') }}
    
),

contests as (

    select * 
    from {{ ref('stg_betway_picks__contests') }} 
    -- where contest_status != 'DRAFT'

),

leagues as (

    select *
    from {{ ref('stg_betway_picks__leagues') }}

),

tiebreak_outcomes as (

    select 
        contest_id, 
        correct_value
    from {{ ref('stg_betway_picks__questions') }}
    where question_type = 'tiebreaker'

),

aggregate_selections_to_entries as (

    select
        user_id || '-' || contest_id as entry_id,
        user_id,
        contest_id,
        sum(points) as points,
        max(tiebreaker_prediction) as tiebreaker_prediction,
        min(created_date) as entry_date,
        day(min(created_date)) as entry_day,
        min(created_at) as entered_at,
        min(created_date_et) as entry_date_et,
        day(min(created_date_et)) as entry_day_et,
        min(created_at_et) as entered_at_et,
        rank() over(
            partition by user_id
            order by entered_at_et
        ) as user_entry_number
    from user_selections 
    group by 1,2,3

),

join_tables as (

    select
        entries.entry_id,
        entries.user_id,
        users.sso_user_id as betway_id,
        users.betway_UserId,
        users.betway_CasinoId,
        users.betway_SubscriberKey,
        entries.contest_id,

        users.username,
        users.registration_date_et,

        contests.contest_title,
        contests.contest_status,
        contests.contest_prize,
        contests.contest_start_date,
        contests.contest_starts_at,
        contests.contest_start_date_et,
        contests.contest_starts_at_et,

        leagues.league_name as region,

        entries.points,
        entries.tiebreaker_prediction,
        tiebreak.correct_value as tiebreaker_outcome,
        round(abs(entries.tiebreaker_prediction - tiebreak.correct_value)) as tiebreaker_error,
        entries.user_entry_number,

        case 
            when user_entry_number = 1
                then 'First Entry'
            else 'Subsequent Entry'
        end as user_entry_type,

        entries.entry_date,
        entries.entry_day,
        hour(entries.entered_at) as entry_hour,
        entries.entered_at,
        entries.entry_date_et,
        entries.entry_day_et,
        hour(entries.entered_at_et) as entry_hour_et,
        entries.entered_at_et

    from aggregate_selections_to_entries as entries 
        inner join users
            on entries.user_id = users.user_id
        inner join contests 
            on entries.contest_id = contests.contest_id
        left join tiebreak_outcomes as tiebreak 
            on entries.contest_id = tiebreak.contest_id
        left join leagues
            on contests.league_id = leagues.league_id

)

select * from join_tables


