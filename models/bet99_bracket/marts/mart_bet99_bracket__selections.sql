with

selections as (

    select *
    from {{ ref('fct_bet99_bracket__selections') }}

),

matches as (

    select *
    from {{ ref('dim_bet99_bracket__matches') }}

),

joined as (

    select
        selections.selection_id,
        selections.entry_id,
        selections.match_id,
        selections.selected_team_id,
        matches.conference_id,
        matches.round_id,
        matches.home_team_id,
        matches.away_team_id,
        matches.winner_team_id,

        matches.contest_name,

        matches.conference_name,

        matches.round_name,
        matches.round_points,

        matches.home_team_name,
        matches.away_team_name,
        matches.winning_team_name,
        selections.selected_team_name,
        matches.match_slot,
        case
            when matches.winning_team_name = selections.selected_team_name
                then true
            else false
        end as is_correct

    from selections
    left join matches
        on selections.match_id = matches.match_id

)

select * from joined
