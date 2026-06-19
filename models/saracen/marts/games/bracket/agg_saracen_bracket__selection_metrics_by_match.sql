with

selections as (

    select *
    from {{ ref('mart_saracen_bracket__selections') }}

),

match_info as (

    select match_id, tournament_name
    from {{ ref('dim_saracen_bracket__matches') }}

),

joined as (

    select
        selections.match_id,
        match_info.tournament_name as contest_name,
        selections.conference_name,
        selections.round_name,
        selections.match_slot,
        selections.home_team_name,
        selections.away_team_name,
        selections.winning_team_name,
        selections.is_correct
    from selections
    left join match_info
        on selections.match_id = match_info.match_id

),

match_stats as (

    select
        match_id,
        contest_name,
        conference_name,
        round_name,
        match_slot,
        home_team_name,
        away_team_name,
        winning_team_name,
        count(*) as total_selections,
        sum(case when is_correct then 1 else 0 end) as correct_selections,
        div0(
            sum(case when is_correct then 1 else 0 end),
            count(*)
        ) as accuracy_rate
    from joined
    group by 1, 2, 3, 4, 5, 6, 7, 8

)

select * from match_stats
