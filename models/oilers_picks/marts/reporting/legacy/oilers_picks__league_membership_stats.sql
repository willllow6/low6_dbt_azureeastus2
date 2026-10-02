-- Compatibility view for the Oilers Picks Tableau dashboard. Repoint the
-- dashboard to rpt_oilers_picks__friend_league_joins_daily, then delete this model.

with

friend_league_joins as (

    select *
    from {{ ref('rpt_oilers_picks__friend_league_joins_daily') }}

)

select
    friend_league_name as league_name,
    friend_league_type as league_type,
    joined_date_et as league_joined_date_et,
    friend_league_created_date_et as league_created_date_et,
    sum(members_joined) as league_members

from friend_league_joins
group by 1, 2, 3, 4
