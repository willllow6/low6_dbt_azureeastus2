with

bracket_selections as (

    select *
    from {{ ref('mart_bet99_bracket__selections') }}

),

match_info as (

    select match_id, tournament_name
    from {{ ref('dim_bet99_bracket__matches') }}

),

joined as (

    select
        match_info.tournament_name as contest_name,
        bracket_selections.conference_name,
        bracket_selections.round_name,
        bracket_selections.match_slot,
        bracket_selections.selected_team_name
    from bracket_selections
    left join match_info
        on bracket_selections.match_id = match_info.match_id

),

agg_bracket_selections as (

    select
        contest_name,
        conference_name,
        round_name,
        match_slot,
        selected_team_name,
        count(*) as selections
    from joined
    group by 1, 2, 3, 4, 5

)

select * from agg_bracket_selections
