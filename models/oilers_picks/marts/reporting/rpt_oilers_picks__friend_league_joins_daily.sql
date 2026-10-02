with

members as (

    select *
    from {{ ref('fct_oilers_picks__friend_league_members') }}

),

friend_leagues as (

    select *
    from {{ ref('dim_oilers_picks__friend_leagues') }}

)

select
    members.friend_league_id,
    members.joined_date_et,

    members.client_id,
    members.tenant_id,
    members.tenant_name,
    members.game_type,

    friend_leagues.friend_league_name,
    friend_leagues.friend_league_type,
    friend_leagues.created_date_et as friend_league_created_date_et,

    count(members.user_id) as members_joined

from members
left join friend_leagues
    on members.friend_league_id = friend_leagues.friend_league_id
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
