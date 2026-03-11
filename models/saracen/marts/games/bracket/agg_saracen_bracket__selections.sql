with

bracket_selections as (

    select *
    from {{ ref('mart_saracen_bracket__selections') }}

),

agg_bracket_selections as (

    select
        contest_name,
        conference_name,
        round_name,
        match_slot,
        selected_team_name,
        count(*) as selections
    from bracket_selections
    group by 1,2,3,4,5

)

select * from agg_bracket_selections