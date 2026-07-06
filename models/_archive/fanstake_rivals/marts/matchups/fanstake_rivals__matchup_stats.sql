with

matchups as (

    select *
    from {{ ref('fanstake_rivals__matchups') }}

),

matchup_stats as (

    select
        created_date_et,
        matchup_type,
        contest_name,
        contest_sport,
        contest_season,
        contest_week_number,
        contest_status,
        contest_league,
        contest_start_date_et,
        count(*) as matchups
    from matchups
    group by 1,2,3,4,5,6,7,8,9

)

select * from matchup_stats